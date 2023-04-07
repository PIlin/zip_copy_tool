using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.IO;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace CopyTool
{
    class Program
    {
		private static readonly NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

		private static char[] s_dirSeparators = new char[] { Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar };

		class Settings
		{
            public string fileCachePath = "";
			public bool copyZipFuzzy = true;
        }

        private ActionBlock<CopyFileRequest> m_copyFileBlock;

        Settings m_settings = new Settings();
		PakFileCache.FileCache m_fileCache;

		static void InitLog()
		{
			var config = new NLog.Config.LoggingConfiguration();
			var layout = new NLog.Layouts.SimpleLayout("${longdate}|${threadid}|${level:uppercase=true}|${logger}|${message} ${exception:format=tostring}");
			var logfile = new NLog.Targets.FileTarget("logfile") { FileName = "log.txt", Layout = layout };
			var logconsole = new NLog.Targets.ConsoleTarget("logconsole") { Layout = layout };
			
			config.AddRule(NLog.LogLevel.Debug, NLog.LogLevel.Fatal, new NLog.Targets.Wrappers.AsyncTargetWrapper(logconsole));
			config.AddRule(NLog.LogLevel.Debug, NLog.LogLevel.Fatal, new NLog.Targets.Wrappers.AsyncTargetWrapper(logfile));
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

		static string NormalizePath(string path)
		{
			path = Path.GetFullPath(path);
			path = path.TrimEnd(s_dirSeparators);
			return path;
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

			NLog.LogManager.Flush();
        }


		void Exec(string[] args)
		{
			int i = 0;
			while (i < args.Length - 2)
			{
				if (args[i] == "-c" || args[i] == "--cache")
				{
					if (args[i + 1][0] != '-')
					{
						m_settings.fileCachePath = NormalizePath(args[i + 1]);
						i += 2;
					}
					else
						throw new ArgumentException("--cache requires file path", "cache");
				}
				else if (args[i] == "--fuzzy")
				{
					m_settings.copyZipFuzzy = true;
					i += 1;
				}
                else if (args[i] == "--no-fuzzy")
                {
                    m_settings.copyZipFuzzy = false;
                    i += 1;
                }
				else
				{
					logger.Warn("Unknown argument {0}: {1}", i, args[i]);
					i++;
				}
            }

			if (args.Length < i + 2)
			{
				throw new ArgumentException("srcPath and dstPath is missing");
			}

			string srcPath = NormalizePath(args[i++]);
			string dstPath = NormalizePath(args[i++]);

			InitFileCache(m_settings.fileCachePath);

            m_copyFileBlock = new ActionBlock<CopyFileRequest>(CopyFile);

            Copy(srcPath, dstPath);

			m_copyFileBlock.Complete();
			logger.Info("Awaiting copy completion");
			m_copyFileBlock.Completion.Wait();
            logger.Info("Copy completed");


            PakFileCache.StreamStatsMgr.Instance.LogReports();
            PakFileCache.StreamStatsMgr.Instance.Reset();
        }

		void InitFileCache(string fileCachePath)
		{
			EPathType cachePathType = CheckPathType(fileCachePath);
			if (cachePathType == EPathType.File || cachePathType == EPathType.None)
			{
				logger.Info($"Invalid file cache path {fileCachePath}, working without cache");
				m_fileCache = new PakFileCache.FileCache();
				return;
			}

			m_fileCache = new PakFileCache.FileCache(fileCachePath);

			m_fileCache.SmallFileSize = 50;
			m_fileCache.DefaultCacheIdGenerator = new PakFileCache.CacheIdGeneratorSimple();
			m_fileCache.ExcludeNamePatterns.Add(new Regex(@"Cry.+\.(pdb|exe|dll)"));
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
			{
				m_copyFileBlock.Post(new CopyFileRequest { srcPath = srcPath, dstPath = dstPath, dstPathType = dstPathType });
			}
			else if (srcPathType == EPathType.Dir)
				CopyDir(srcPath, dstPath);
		}

		class CopyFileRequest
		{
			public string srcPath;
			public string dstPath;
			public EPathType dstPathType;
        }
		void CopyFile(CopyFileRequest req)
		{
			string srcExt = Path.GetExtension(req.srcPath);
			if (srcExt == ".zip" || srcExt == ".pak")
			{
				try
				{
					if (m_fileCache.Enabled)
					{
                        logger.Info("Replicate zip {0} to {1}", req.srcPath, req.dstPath);
                        PakFileCache.ZipReplicate.ReplicateZipFileWithCache(req.srcPath, req.dstPath, m_fileCache);
						return;
                    }

					if (req.dstPathType != EPathType.None)
					{

						if (m_settings.copyZipFuzzy)
						{
							logger.Info("Replicate zip {0} to {1} with fuzzy update", req.srcPath, req.dstPath);
							PakFileCache.ZipReplicate.ReplicateUpdateFuzzy(req.srcPath, req.dstPath);
						}
						else
						{
							logger.Info("Replicate zip {0} to {1} with update", req.srcPath, req.dstPath);
							PakFileCache.ZipReplicate.ReplicateUpdate(req.dstPath, req.srcPath, req.dstPath);
						}
						return;
					}
					Debug.Assert(req.dstPathType == EPathType.None);
				}
				catch (Exception e)
				{
					logger.Warn(e, "Failed to replicate zip file, retry as normal file copy");
				}
			}
			
			{
				logger.Info("Copy {0} to {1}", req.srcPath, req.dstPath);
				m_fileCache.CopyFile(req.srcPath, req.dstPath);
			}
		}

		void CopyDir(string srcPath, string dstPath)
		{
			logger.Info("Enumerating files in {0}", srcPath);
			List<string> srcFiles = new List<string>();
			List<string> dstFiles = new List<string>();
			HashSet<string> dstDirs = new HashSet<string>();

            foreach (string srcFilepath in Directory.EnumerateFiles(srcPath, "*", SearchOption.AllDirectories))
			{
				string srcDir = Path.GetDirectoryName(srcFilepath);
				if (srcDir.StartsWith(srcPath))
				{
					string srcRelDir = srcDir.Remove(0, srcPath.Length).Trim(s_dirSeparators);
					string dstDir = Path.Combine(dstPath, srcRelDir);
					string dstFilepath = Path.Combine(dstDir, Path.GetFileName(srcFilepath));

					srcFiles.Add(srcFilepath);
					dstFiles.Add(dstFilepath);
					dstDirs.Add(dstDir);

				}
				else
                {
					logger.Warn("Unable to retrieve path of file {0} relative to {1}", srcFilepath, srcPath);
				}
			}

			Debug.Assert(srcFiles.Count == dstFiles.Count);
            logger.Info("Found {0} files", srcFiles.Count);

			foreach (string dir in dstDirs)
			{
                Directory.CreateDirectory(dir);
            }

			for (int i = 0; i < srcFiles.Count; ++i)
			{
				Copy(srcFiles[i], dstFiles[i]);
			}
        }

	}
}
