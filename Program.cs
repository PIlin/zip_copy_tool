using System.IO;
using System.Security.Cryptography;

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

			fc.Add(@"e:\photo\2020_Ma\IMG-8436bb0cf2bceb815b2065ee9ea4beb5-V.jpeg.jpg", null).CopyToFile(@"f:\test_target\test.jpg");
		}

		static void ReplicateZipFileWithCache(string src, string dst, PakFileCache.FileCache fc)
		{
			PakFileCache.ZipReplicate.ReplicateZipFileWithCache(src, dst, fc);
			PakFileCache.StreamStatsMgr.Instance.LogReports();
			PakFileCache.StreamStatsMgr.Instance.Reset();
		}

		static void TestZipCacheReplicate()
		{
			var fc = new PakFileCache.FileCache(@"f:\testcache") { SmallFileSize = 0 };

			ReplicateZipFileWithCache(@"d:\code\PakPatcher\test\v1.zip", @"d:\code\PakPatcher\test\v1_out.zip", fc);
			ReplicateZipFileWithCache(@"d:\code\PakPatcher\test\v2.zip", @"d:\code\PakPatcher\test\v2_out.zip", fc);
		}

        static void TestZipReplicate()
		{
            PakFileCache.ZipReplicate.ReplicateUpdate(@"d:\code\PakPatcher\test\v1.zip", @"d:\code\PakPatcher\test\v2.zip", @"d:\code\PakPatcher\test\v1_2.zip");
            PakFileCache.StreamStatsMgr.Instance.LogReports();
            PakFileCache.StreamStatsMgr.Instance.Reset();
        }

        static void TestZipReplicateFuzzy()
        {
			string v1 = @"d:\code\PakPatcher\test\v1.zip";
			string v2 = @"d:\code\PakPatcher\test\v2.zip";
            string dst = @"d:\code\PakPatcher\test\v1_2.zip";
			try
			{
				File.Delete(dst);
			}
			catch { }
			File.Copy(v1, dst);

            PakFileCache.ZipReplicate.ReplicateUpdateFuzzy(v2, dst);
            PakFileCache.StreamStatsMgr.Instance.LogReports();
            PakFileCache.StreamStatsMgr.Instance.Reset();
        }

        static void Main(string[] args)
        {
			InitLog();

			//TestZipReplicate();
			TestZipReplicateFuzzy();

            //TestCacheCopy();
            //TestZipCacheReplicate();
        }
    }
}
