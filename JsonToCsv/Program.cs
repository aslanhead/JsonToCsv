namespace JsonToCsv
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using MongoDB.Bson;

    class Program
    {
        private static bool countsOnly = false;

        static void Main(string[] args)
        {
            Console.WriteLine("Enter JSON directory path, if skipped will use current directory:");
            string jsonDirectoryPath = Console.ReadLine();
            if (string.IsNullOrEmpty(jsonDirectoryPath) || jsonDirectoryPath.Equals("."))
            {
                jsonDirectoryPath = Directory.GetCurrentDirectory();
            }

            if (!Directory.Exists(jsonDirectoryPath))
            {
                Console.WriteLine("Directory path is incorrect" + jsonDirectoryPath);
                return;
            }

            Console.WriteLine();
            Console.WriteLine("Enter delimiter (if ignored, comma is used):");
            string firstDelimiter = Console.ReadLine();
            if (string.IsNullOrEmpty(firstDelimiter))
            {
                firstDelimiter = ",";
            }

            Console.WriteLine();
            Console.WriteLine("Enter second delimiter (optional). If your data contains characters like the first delimiter, use the second one");
            string secondDelimiter = Console.ReadLine();

            Console.WriteLine();
            Console.WriteLine("Enter the comma separated header for the file:");
            string header = Console.ReadLine();
            if (string.IsNullOrEmpty(header))
            {
                Console.WriteLine("No data input to fetch");
                return;
            }

            Console.WriteLine();
            Console.WriteLine("Do you need counts only on enumerables (Y/N)?");
            string answer = Console.ReadLine();
            if (answer == "Y" || answer == "y")
            {
                countsOnly = true;
            }

            Console.WriteLine();
            Console.WriteLine("Enter batch size for each file, (no batches by default):");
            string batchSizeString = Console.ReadLine();
            int batchSize;
            if (int.TryParse(batchSizeString, out batchSize))
            {
                if (batchSize <= 0)
                {
                    Console.WriteLine("Batch size should be positive");
                    return;
                }
            }

            if (batchSize <= 0)
            {
                batchSize = -1;
            }
            Console.WriteLine("Do you need the fileName on each row (Y/N)?");
            answer = Console.ReadLine();
            bool fileNameNeeded = false;
            if (answer == "Y" || answer == "y")
            {
                fileNameNeeded = true;
            }
            foreach (string s in Directory.GetFiles(jsonDirectoryPath, "j_results*"))
            {
                File.Delete(s);
            }

            Console.WriteLine("Started processing at " + DateTime.Now);
            string[] headerInOrder = header.Split(',');
            Dictionary<string, List<int>> headerToIndexesMap = new Dictionary<string, List<int>>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < headerInOrder.Length; i++)
            {
                if (!headerToIndexesMap.ContainsKey(headerInOrder[i]))
                {
                    headerToIndexesMap[headerInOrder[i]] = new List<int>() { i };
                }
                else
                {
                    headerToIndexesMap[headerInOrder[i]].Add(i);
                }
            }

            StringBuilder sb = new StringBuilder();
            string h = string.Empty;
            if (fileNameNeeded)
            {
                h = "FileName" + firstDelimiter;
            }
            h += string.Join(firstDelimiter,headerInOrder);
            sb.AppendLine(h);
            int currentFileNumber = 0;
            foreach (string fullName in Directory.GetFiles(jsonDirectoryPath, "*.json"))
            {
                currentFileNumber++;
                try
                {
                    BsonDocument document = BsonDocument.Parse(File.ReadAllText(fullName));
                    var sorted = new SortedDictionary<int, string>();
                    sb.Append(Path.GetFileName(fullName) + firstDelimiter);
                    Recurse(document, headerToIndexesMap, sorted, firstDelimiter, secondDelimiter);
                    for (int i = 0; i < headerInOrder.Length; i++)
                    {
                        if (sorted.ContainsKey(i))
                        {
                            sb.Append(sorted[i] + firstDelimiter);
                        }
                        else
                        {
                            sb.Append(firstDelimiter);
                        }
                    }

                    sb.Length = sb.Length - firstDelimiter.Length;
                    sb.AppendLine();
                }
                catch
                {
                    Console.WriteLine("Could not parse file " + fullName);
                }

                if (batchSize > 0)
                {
                    if (currentFileNumber % batchSize == 0)
                    {
                        Console.WriteLine("Finished processing " + currentFileNumber + " files");
                        int q = currentFileNumber / batchSize;
                        File.WriteAllText("j_results_" + q + ".txt", sb.ToString());
                        sb.Clear();
                    }
                }
            }
            if (sb.Length > 0)
            {
                File.WriteAllText("j_results_final.txt", sb.ToString());
            }

            string currentDirectory = Directory.GetCurrentDirectory();
            List<FileInfo> fileInfos = Directory.GetFiles(currentDirectory, "j_results*.txt")
                .Select(f => new FileInfo(f)).ToList();
            fileInfos.Sort(new FileInfoComparer());
            StreamWriter sw = new StreamWriter("results.txt", false);
            foreach (FileInfo fInfo in fileInfos)
            {
                using (FileStream fs = new FileStream(fInfo.FullName, FileMode.Open, FileAccess.Read))
                {
                    using (StreamReader sr = new StreamReader(fs))
                    {
                        while (!sr.EndOfStream)
                        {
                            sw.WriteLine(sr.ReadLine());
                        }
                    }

                    sw.Flush();
                }
            }

            sw.Flush();

            Console.WriteLine();
            Console.WriteLine("Results are written to results.txt");
            Console.WriteLine("Finished processing at " + DateTime.Now);
            Console.WriteLine();
        }

        private static void Recurse(BsonDocument document, Dictionary<string, List<int>> headerToIndexesMap, SortedDictionary<int, string> sd, string first, string second)
        {
            foreach (BsonElement element in document)
            {
                if (element.Value is BsonDocument)
                {
                    Recurse(element.Value.AsBsonDocument, headerToIndexesMap, sd, first, second);
                }

                string value = element.Value.ToString();
                if (countsOnly)
                {
                    var x = element.Value as IEnumerable<BsonElement>;
                    if (x != null)
                    {
                        value = x.Count().ToString();
                    }
                }

                if (!string.IsNullOrEmpty(second))
                {
                    value = value.Replace(first, second);
                }
                if (headerToIndexesMap.ContainsKey(element.Name))
                {
                    List<int> temp = headerToIndexesMap[element.Name];
                    foreach (var i in temp)
                    {
                        sd[i] = value;
                    }
                }
            }
        }
    }

    public class FileInfoComparer : IComparer<FileInfo>
    {
        public int Compare(FileInfo x, FileInfo y)
        {
            if (x == null || y == null)
            {
                throw new ArgumentNullException();
            }
            return x.LastWriteTimeUtc.CompareTo(y.LastWriteTimeUtc);
        }
    }
}
