using System;
using System.Buffers;
using System.IO;
using System.IO.Pipelines;
using System.Text;
using System.Threading.Tasks;

namespace SplitData
{
	public class TableDataProcessor
	{
		private const int BufferSize = 1024 * 256;
		private readonly string _inputPath;
		private readonly string _tableOutputPath;
		private FileStream _currentOutput;
		private static readonly byte[] StartOfData = Encoding.UTF8.GetBytes("-- Data for Name: ");
		private static readonly byte[] NewLine = {(byte) '\n'};

		public TableDataProcessor(string inputPath) {
			_inputPath = inputPath;
			_tableOutputPath = Path.Combine(Path.GetDirectoryName(inputPath), "tables");
			Directory.CreateDirectory(_tableOutputPath);
		}

		private async Task ProcessLinesAsync(Stream stream) {
			var reader = PipeReader.Create(stream);

			while( true ) {
				var result = await reader.ReadAsync();
				var buffer = result.Buffer;

				while( TryReadLine(ref buffer, out var line) ) {
					ProcessLine(line);
					await line.CopyToAsync(_currentOutput);
					await _currentOutput.WriteAsync(NewLine);
				}

				// Tell the PipeReader how much of the buffer has been consumed.
				reader.AdvanceTo(buffer.Start, buffer.End);

				if( result.IsCompleted ) {
					break;
				}
			}

			await reader.CompleteAsync();
		}

		private static bool TryReadLine(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> line) {
			var position = buffer.PositionOf((byte) '\n');

			if( position == null ) {
				line = default;
				return false;
			}

			line = buffer.Slice(0, position.Value);
			buffer = buffer.Slice(buffer.GetPosition(1, position.Value));
			return true;
		}

		private void ProcessLine(in ReadOnlySpan<byte> buffer) {
			if( buffer.Length >= StartOfData.Length &&
				buffer.Slice(0, StartOfData.Length).SequenceEqual(StartOfData) ) {
				// `table_name; Type: TABLE DATA; Schema: public; Owner: some_owner`
				var tableName = buffer.Slice(StartOfData.Length);
				tableName = tableName.Slice(0, tableName.IndexOf((byte) ';'));

				var name = Encoding.UTF8.GetString(tableName);
				ReplaceOutput($"{name}.sql");
			}
		}

		private void ProcessLine(in ReadOnlySequence<byte> buffer) {
			if( buffer.IsSingleSegment ) {
				ProcessLine(buffer.FirstSpan);
			}
			else {
				Span<byte> bytes = stackalloc byte[(int) buffer.Length];
				buffer.CopyTo(bytes);
				ProcessLine(bytes);
			}
		}

		private void ReplaceOutput(FileStream stream) {
			_currentOutput?.Dispose();
			_currentOutput = stream;
		}

		private void ReplaceOutput(string fileName) {
			Console.WriteLine($"Writing to {fileName}");
			var path = Path.Combine(_tableOutputPath, fileName);
			ReplaceOutput(new FileStream(
				path,
				FileMode.Create,
				FileAccess.Write,
				FileShare.None,
				BufferSize,
				FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.WriteThrough)
			);
		}

		public async Task RunAsync() {
			ReplaceOutput("_schema.sql");
			try {
				await using var f = new FileStream(_inputPath,
					FileMode.Open,
					FileAccess.Read,
					FileShare.Read,
					BufferSize,
					FileOptions.Asynchronous | FileOptions.SequentialScan);


				await ProcessLinesAsync(f);
			}
			finally {
				if( _currentOutput != null ) {
					await _currentOutput.FlushAsync();
					await _currentOutput.DisposeAsync();
				}
			}
		}
	}


	public static class SequenceExtensions
	{
		public static ValueTask CopyToAsync(in this ReadOnlySequence<byte> source, Stream stream) =>
			source.IsSingleSegment
				? stream.WriteAsync(source.First)
				: CopyToMultiSegment(source, stream);

		private static async ValueTask CopyToMultiSegment(ReadOnlySequence<byte> sequence, Stream stream) {
			var position = sequence.Start;
			while( sequence.TryGet(ref position, out var memory) ) {
				await stream.WriteAsync(memory);
			}
		}
	}
}