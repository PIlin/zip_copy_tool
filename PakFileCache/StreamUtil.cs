using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace PakFileCache
{
	public static class StreamUtil
	{
		public static FileStreamOptions MakeReadAsyncOpts()
		{
			return new FileStreamOptions()
			{
				Mode = FileMode.Open,
				Access = FileAccess.Read,
				Options = FileOptions.Asynchronous
			};
		}
		public static FileStreamOptions MakeWriteAsyncOpts(FileMode mode, FileAccess access = FileAccess.Write, long preallocationSize = 0)
		{
			return new FileStreamOptions()
			{
				Mode = mode,
				Access = access,
				Options = FileOptions.Asynchronous,
				PreallocationSize = preallocationSize
			};
		}

		public static void FillBuffer(Stream stream, byte[] buffer, int numBytes)
		{
			int read = 0;
			while (read < numBytes)
			{
				int n = stream.Read(buffer, read, numBytes - read);
				if (n == 0)
				{
					throw new EndOfStreamException();
				}
				read += n;
			}
		}

		public static async Task FillBufferAsync(Stream stream, byte[] buffer, int numBytes)
		{
			int read = 0;
			while (read < numBytes)
			{
				int n = await stream.ReadAsync(buffer, read, numBytes - read);
				if (n == 0)
				{
					throw new EndOfStreamException();
				}
				read += n;
			}
		}

		public static byte[] ReadBuffer(Stream stream, byte[] buffer, int numBytes)
		{
			if (numBytes == 0) return buffer;
			if (buffer == null || buffer.Length < numBytes)
			{
				buffer = new byte[numBytes];
			}
			FillBuffer(stream, buffer, numBytes);
			return buffer;
		}

		public static async Task<byte[]> ReadBufferAsync(Stream stream, byte[] buffer, int numBytes)
		{
			if (numBytes == 0) return buffer;
			if (buffer == null || buffer.Length < numBytes)
			{
				buffer = new byte[numBytes];
			}
			await FillBufferAsync(stream, buffer, numBytes);
			return buffer;
		}

		public static void CopyNTo(Stream src, Stream dst, long n)
		{
#if false
			CopyNToSync(src, dst, n);
#elif false
			CopyNToAsyncSimple(src, dst, n).Wait();
#elif true
			CopyNToAsyncParallel(src, dst, n).Wait();
#endif
		}

		public static Task CopyNToAsync(Stream src, Stream dst, long n)
		{
#if false
			return CopyNToAsyncSimple(src, dst, n);
#elif true
			return CopyNToAsyncParallel(src, dst, n);
#endif
		}


		private static void CopyNToSync(Stream src, Stream dst, long n)
		{
			if (n > 0)
			{
				// Stream.GetCopyBufferSize() uses this size 81920
				const int bufferSize = 80 * 1024;

				var pool = ArrayPool<byte>.Shared;
				byte[] buffer = pool.Rent(bufferSize);

				try
				{
					int read;
					while (n > 0 &&
						   (read = src.Read(buffer, 0, (int)Math.Min(bufferSize, n))) > 0)
					{
						dst.Write(buffer, 0, read);
						n -= read;
					}
				}
				finally
				{
					pool.Return(buffer);
				}

			}
		}

		private static Task CopyNToAsyncSimple(Stream src, Stream dst, long n)
		{
			if (n > 0)
			{
				return CopyImpl(src, dst, n);

				static async Task CopyImpl(Stream src, Stream dst, long n)
				{
					// Stream.GetCopyBufferSize() uses this size 81920
					const int bufferSize = 80 * 1024;

					var pool = ArrayPool<byte>.Shared;
					byte[] buffer = pool.Rent(bufferSize);

					try
					{
						int read;
						while (n > 0 &&
							   (read = await src.ReadAsync(buffer, 0, (int)Math.Min(bufferSize, n)).ConfigureAwait(false)) > 0)
						{
							await dst.WriteAsync(buffer, 0, read).ConfigureAwait(false);
							n -= read;
						}
					}
					finally
					{
						pool.Return(buffer);
					}
				};
			}

			return Task.CompletedTask;
		}

		private static Task CopyNToAsyncParallel(Stream src, Stream dst, long n)
		{
			if (n > 0)
			{
				return CopyImpl(src, dst, n);

				static async Task CopyImpl(Stream src, Stream dst, long n)
				{
					// Stream.GetCopyBufferSize() uses this size 81920
					const int bufferSize = 80 * 1024;

					var pool = ArrayPool<byte>.Shared;
					byte[] readBuf = pool.Rent(bufferSize);
					byte[] writeBuf = pool.Rent(bufferSize);

					try
					{
						int totalRead = 0;
						int totalWritten = 0;

						Debug.Assert(n > 0);

						var readTask = src.ReadAsync(readBuf, 0, (int)Math.Min(bufferSize, n)).ConfigureAwait(false);
						int read;
						while (n > 0 && (read = await readTask) > 0)
						{
							n -= read;
							totalRead += read;
							(readBuf, writeBuf) = (writeBuf, readBuf);

							// run read and write operation concurrently
							if (n > 0)
								readTask = src.ReadAsync(readBuf, 0, (int)Math.Min(bufferSize, n)).ConfigureAwait(false);

							await dst.WriteAsync(writeBuf, 0, read).ConfigureAwait(false);
							totalWritten += read;
						}
						Debug.Assert(totalRead == totalWritten);
					}
					finally
					{
						pool.Return(readBuf);
						pool.Return(writeBuf);
					}
				};
			}
			return Task.CompletedTask;
		}
	}

}
