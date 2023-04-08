using System.Diagnostics;
using System.IO.Compression;
using System.IO.Hashing;
using System.Text;

namespace TestGen;
class Program
{
    enum GenEvent { New, Del, ModSame, ModDiff, Keep };

    static int GetStringHash(string s)
    {
        Crc32 crc = new Crc32();
        var bytes = Encoding.UTF8.GetBytes(s);
        crc.Append(bytes);
        var hash =  crc.GetCurrentHash();
        return BitConverter.ToInt32(hash);
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
                rnd.NextBytes(s);
                string ename = $"f{i}.dat";

                Console.Write($"{ename}: {genEvent}, size = {size}");

                if (genEvent != GenEvent.New)
                {
                    using (var e = ar1.CreateEntry(ename, CompressionLevel.NoCompression).Open())
                    {
                        e.Write(s);
                    }
                }

                if (genEvent != GenEvent.Del)
                {
                    if (genEvent == GenEvent.ModSame)
                    {
                        rnd.NextBytes(s);
                    }
                    else if (genEvent == GenEvent.ModDiff)
                    {
                        int sizeV2 = rnd.Next(sizeMin, sizeMax);
                        s = new Span<byte>(buffer, 0, sizeV2);
                        rnd.NextBytes(s);
                        Console.Write($", sizeV2 = {sizeV2}");
                    }

                    using (var e = ar2.CreateEntry(ename, CompressionLevel.NoCompression).Open())
                    {
                        e.Write(s);
                    }
                }

                Console.WriteLine();
            }
        }
        Console.WriteLine();
    }

    static void Main(string[] args)
    {
        if (args.Length < 4)
        {
            Console.Error.WriteLine("Unexpected number of arguments");
            return;
        }

        string v1 = args[1];
        string v2 = args[2];

        Directory.CreateDirectory(v1);
        Directory.CreateDirectory(v2);

        for (int i = 2; i < args.Length; i++)
        {
            string filename = args[i];
            GenerateFile(v1, v2, filename);
        }
    }
}
