using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace PakFileCache
{
	public static class StreamUtil
	{
		public static void FillBuffer(Stream stream, byte[] buffer, int numBytes)
		{
			int read = 0;
			do
			{
				int n = stream.Read(buffer, read, numBytes - read);
				if (n == 0)
				{
					throw new EndOfStreamException();
				}
				read += n;
			} while (read < numBytes);
		}

		public static byte[] ReadBuffer(Stream stream, byte[] buffer, int numBytes)
		{
			if (numBytes == 0) return buffer;
			if (buffer?.Length < numBytes)
			{
				buffer = new byte[numBytes];
			}
			FillBuffer(stream, buffer, numBytes);
			return buffer;
		}

		public static void CopyNTo(Stream src, Stream dst, long n)
		{
			if (n > 0)
			{
				const long bufferSize = 64 * 1024;
				byte[] buffer = new byte[bufferSize];
				int read;
				while (n > 0 &&
					   (read = src.Read(buffer, 0, (int)Math.Min(bufferSize, n))) > 0)
				{
					dst.Write(buffer, 0, read);
					n -= read;
				}
			}
		}

	}

}
