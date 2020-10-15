using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace CopyTool
{
    class Program
    {
		private static readonly NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

		PakFileCache.FileCache m_fileCache;

		static void InitLog()
		{
			var config = new NLog.Config.LoggingConfiguration();
			var layout = new NLog.Layouts.SimpleLayout("${longdate}|${level:uppercase=true}|${logger}|${message} ${exception:format=tostring}");
			var logfile = new NLog.Targets.FileTarget("logfile") { FileName = "log.txt", Layout = layout };
			var logconsole = new NLog.Targets.ConsoleTarget("logconsole") { Layout = layout };
			config.AddRule(NLog.LogLevel.Debug, NLog.LogLevel.Fatal, logconsole);
			config.AddRule(NLog.LogLevel.Debug, NLog.LogLevel.Fatal, logfile);
			NLog.LogManager.Configuration = config;
		}

		enum EPathType
        {
			None,
			File,
			Dir
        };

		static EPathType CheckPathType(string path)
		{
			if (Directory.Exists(path)) return EPathType.Dir;
			if (File.Exists(path)) return EPathType.File;
			return EPathType.None;
		}

		static void Main(string[] args)
        {
			try
			{
				InitLog();
				Program p = new Program();
				p.Exec(args);
			}
			catch (Exception ex)
			{
				logger.Error(ex, "Failed");
				Environment.ExitCode = -1;
			}
		}


		void Exec(string[] args)
		{
			string fileCachePath = "";

			int i = 0;
			while (i < args.Length - 2)
			{
				if (args[i] == "-c" || args[i] == "--cache")
				{
					if (args[i + 1][0] != '-')
					{
						fileCachePath = args[i + 1];
						i += 2;
					}
					else
						throw new ArgumentException("--cache requires file path", "cache");
				}
			}

			if (args.Length < i + 2)
			{
				throw new ArgumentException("srcPath and dstPath is missing");
			}

			string srcPath = args[i++];
			string dstPath = args[i++];

			InitFileCache(fileCachePath);

			Copy(srcPath, dstPath);
		}

		void InitFileCache(string fileCachePath)
		{
			EPathType cachePathType = CheckPathType(fileCachePath);
			if (cachePathType == EPathType.File || cachePathType == EPathType.None)
				throw new ArgumentException($"Invalid file cache path {fileCachePath}", "cache");

			m_fileCache = new PakFileCache.FileCache(fileCachePath);
		}

		void Copy(string srcPath, string dstPath)
		{
			EPathType srcPathType = CheckPathType(srcPath);
			if (srcPathType == EPathType.None)
				throw new FileNotFoundException("srcFile not found", srcPath);

			EPathType dstPathType = CheckPathType(dstPath);
			if (dstPathType != EPathType.None && srcPathType != dstPathType)
				throw new ArgumentException($"srcFile and dstPath have different types: src {srcPathType}, dst {dstPathType}");

			if (srcPathType == EPathType.File)
				CopyFile(srcPath, dstPath);
			else if (srcPathType == EPathType.Dir)
				CopyDir(srcPath, dstPath);

			PakFileCache.StreamStatsMgr.Instance.LogReports();
			PakFileCache.StreamStatsMgr.Instance.Reset();
		}

		void CopyFile(string srcPath, string dstPath)
		{
			string srcExt = Path.GetExtension(srcPath);
			if (srcExt == ".zip" || srcExt == ".pak")
			{
				logger.Info("Copy zip {0} to {1}", srcPath, dstPath);
				PakFileCache.ZipReplicate.ReplicateZipFileWithCache(srcPath, dstPath, m_fileCache);
			}
			else
			{
				logger.Info("Copy {0} to {1}", srcPath, dstPath);
				m_fileCache.CopyFile(srcPath, dstPath);
			}
		}

		void CopyDir(string srcPath, string dstPath)
		{
			throw new NotImplementedException();
		}
	}
}
