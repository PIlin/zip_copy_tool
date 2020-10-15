namespace PakPatcher
{
    class Program
	{
		private static readonly NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

		static void InitLog()
		{
			var config = new NLog.Config.LoggingConfiguration();
			var logfile = new NLog.Targets.FileTarget("logfile") { FileName = "log.txt" };
			var logconsole = new NLog.Targets.ConsoleTarget("logconsole");
			config.AddRule(NLog.LogLevel.Debug, NLog.LogLevel.Fatal, logconsole);
			config.AddRule(NLog.LogLevel.Debug, NLog.LogLevel.Fatal, logfile);
			NLog.LogManager.Configuration = config;
		}

		static void TestCacheCopy()
		{
			var fc = new PakFileCache.FileCache(@"f:\testcache");

			fc.Add(@"e:\photo\2020_Ma\IMG-8436bb0cf2bceb815b2065ee9ea4beb5-V.jpeg.jpg").CopyToFile(@"f:\test_target\test.jpg");
		}

		static void TestZipCacheReplicate()
		{
			var fc = new PakFileCache.FileCache(@"f:\testcache");

			PakFileCache.ZipReplicate.ReplicateZipFileWithCache(@"d:\code\PakPatcher\test\v1.zip", @"d:\code\PakPatcher\test\v1_out.zip", fc);
			PakFileCache.ZipReplicate.ReplicateZipFileWithCache(@"d:\code\PakPatcher\test\v2.zip", @"d:\code\PakPatcher\test\v2_out.zip", fc);
		}


		static void Main(string[] args)
        {
			InitLog();

			//TestZipReplicate();

			//TestCacheCopy();
			TestZipCacheReplicate();
		}
    }
}
