using System;
using System.IO;
using System.Threading.Tasks;

namespace SplitData
{
	internal static class Program
	{
		static Task Main(string[] args) {
			if( args.Length != 1 ) {
				Console.Error.WriteLine("App takes a single argument, which is the input file");
				Environment.Exit(1);
				return Task.CompletedTask;
			}

			var inputFileName = args[0];
			if( !File.Exists(inputFileName) ) {
				Console.Error.WriteLine("Input file {0} does not exist", inputFileName);
				Environment.Exit(1);
				return Task.CompletedTask;
			}

			return new TableDataProcessor(inputFileName)
				.RunAsync();
		}
	}
}