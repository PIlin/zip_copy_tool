using System;
using System.Diagnostics;
using System.IO.Compression;
using System.IO.Hashing;
using System.Text;

namespace TestGen;
class Program
{
    enum GenEvent { New, Del, ModSame, ModDiff, Keep };

    static Stopwatch s_bufGenTime = new Stopwatch();
    static long s_bufGenSize = 0;

    static int GetStringHash(string s)
    {
        Crc32 crc = new Crc32();
        var bytes = Encoding.UTF8.GetBytes(s);
        crc.Append(bytes);
        var hash =  crc.GetCurrentHash();
        return BitConverter.ToInt32(hash);
    }

    static DateTimeOffset GenerateDTO(Random rnd, int min, int max)
    {
        int modtime = rnd.Next(min, max);
        DateTimeOffset dto = DateTimeOffset.FromUnixTimeSeconds(modtime);
        return dto;
    }


    static UInt64 xorshift64(UInt64 x)
    {
        /* see https://en.wikipedia.org/wiki/Xorshift
        struct xorshift64_state
        {
            uint64_t a;
        };
        uint64_t xorshift64(struct xorshift64_state *state)
        {
            uint64_t x = state->a;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            return state->a = x;
        }
        */

        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        return x;
    }

    static void GenerateBufferXorshift(UInt64 state, Span<byte> buffer)
    {
        int alignedLen = (buffer.Length / 8) * 8;
        Span<byte> bufAligned = buffer.Slice(0, alignedLen);
        Span<byte> bufTail = buffer.Slice(alignedLen);

        for (int offset = 0; offset < bufAligned.Length; offset += 8)
        {
            UInt64 x = xorshift64(state);
            bufAligned[offset + 0] = (byte)(x & 0xFF);
            bufAligned[offset + 1] = (byte)((x >> 8) & 0xFF);
            bufAligned[offset + 2] = (byte)((x >> 16) & 0xFF);
            bufAligned[offset + 3] = (byte)((x >> 24) & 0xFF);
            bufAligned[offset + 4] = (byte)((x >> 32) & 0xFF);
            bufAligned[offset + 5] = (byte)((x >> 40) & 0xFF);
            bufAligned[offset + 6] = (byte)((x >> 48) & 0xFF);
            bufAligned[offset + 7] = (byte)((x >> 56) & 0xFF);

            state = x;
        }


        if (bufTail.Length > 0)
        {
            UInt64 x = xorshift64(state);

            for (int offset = 0; offset < bufTail.Length; offset++)
            {
                int shift = 8 * (offset % 8);
                bufTail[offset] = (byte)((x >> shift) & 0xFF);
            }
        }
    }

    static void GenerateBuffer(Random rnd, Span<byte> buffer)
    {
        s_bufGenTime.Start();

#if false
        // very slow, around ~180 MB/s
        rnd.NextBytes(buffer);
#else
        // does ~3 GB/s
        GenerateBufferXorshift((ulong)rnd.NextInt64(), buffer);
#endif

        s_bufGenTime.Stop();
        s_bufGenSize += buffer.Length;
    }


    static void GenerateFile(string pathV1, string pathV2, string filename)
    {
        int seed = GetStringHash(filename);

        Console.WriteLine($"Generating {filename}, seed {seed}");

        pathV1 = Path.Combine(pathV1, filename);
        pathV2 = Path.Combine(pathV2, filename);

        if (File.Exists(pathV1)) { File.Delete(pathV1); }
        if (File.Exists(pathV2)) { File.Delete(pathV2); }

        int entriesCount = 100;
        int sizeMin = 32 * 1024;
        int sizeMax = 4 * 1024 * 1024;

        int timeMin = 1680976104;
        int timeMax = timeMin + 100000;

        float newChance = 0.05f;
        float delChance = 0.02f;
        float modsameChance = 0.08f;
        float moddiffChance = 0.08f;

        float[] chances = new float[4]
        {
            newChance, delChance, modsameChance, moddiffChance
        };
        for (int i = 1; i < chances.Length; ++i)
        {
            chances[i] += chances[i - 1];
        }
        Debug.Assert(chances.Last() < 1.0f);

        Random rnd = new Random(seed);

        using (var ar1 = ZipFile.Open(pathV1, ZipArchiveMode.Create))
        using (var ar2 = ZipFile.Open(pathV2, ZipArchiveMode.Create))
        {
            byte[] buffer = new byte[sizeMax];
            for (int i = 0; i < entriesCount; i++)
            {
                GenEvent genEvent = GenEvent.Keep;
                float ch = rnd.NextSingle();
                for (int ich = 0; ich < chances.Length; ich++) 
                { 
                    if (ch < chances[ich])
                    {
                        genEvent = (GenEvent)ich;
                        break;
                    }
                }

                int size = rnd.Next(sizeMin, sizeMax);
                var s = new Span<byte>(buffer, 0, size);
                GenerateBuffer(rnd, s);
                string ename = $"f{i}.dat";
                DateTimeOffset dto = GenerateDTO(rnd, timeMin, timeMax);

                Console.Write($"{ename}: {genEvent}, size = {size}");

                if (genEvent != GenEvent.New)
                {
                    var entry = ar1.CreateEntry(ename, CompressionLevel.NoCompression);
                    entry.LastWriteTime = dto;
                    using (var e = entry.Open())
                    {
                        e.Write(s);
                    }
                    
                }

                if (genEvent != GenEvent.Del)
                {
                    if (genEvent == GenEvent.ModSame)
                    {
                        GenerateBuffer(rnd, s);
                        dto = dto.AddSeconds(1 * 60 * 60);
                    }
                    else if (genEvent == GenEvent.ModDiff)
                    {
                        int sizeV2 = rnd.Next(sizeMin, sizeMax);
                        s = new Span<byte>(buffer, 0, sizeV2);
                        GenerateBuffer(rnd, s);
                        Console.Write($", sizeV2 = {sizeV2}");
                        dto = dto.AddSeconds(2 * 60 * 60);
                    }

                    var entry = ar2.CreateEntry(ename, CompressionLevel.NoCompression);
                    entry.LastWriteTime = dto;
                    using (var e = entry.Open())
                    {
                        e.Write(s);
                    }
                }

                Console.WriteLine();
            }
        }
        
        File.SetLastWriteTime(pathV1, DateTimeOffset.FromUnixTimeSeconds(timeMax).DateTime);
        File.SetLastWriteTime(pathV2, DateTimeOffset.FromUnixTimeSeconds(timeMax + 86000).DateTime);

        Console.WriteLine();
    }

    static void Main(string[] args)
    {
        if (args.Length < 3)
        {
            Console.Error.WriteLine("Unexpected number of arguments");
            return;
        }

        Stopwatch sw = Stopwatch.StartNew();

        string v1 = args[0];
        string v2 = args[1];

        Directory.CreateDirectory(v1);
        Directory.CreateDirectory(v2);

        for (int i = 2; i < args.Length; i++)
        {
            string filename = args[i];
            GenerateFile(v1, v2, filename);
        }

        sw.Stop();

        Console.WriteLine($"Time {sw.Elapsed}, gen time {s_bufGenTime.Elapsed}, gen rate { (s_bufGenSize / s_bufGenTime.Elapsed.TotalSeconds) / 1024.0f / 1024.0f }");
    }
}
