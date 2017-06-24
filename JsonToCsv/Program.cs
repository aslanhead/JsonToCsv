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
            Console.WriteLine("Enter JSON directory path, if skipped will use current directory");
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

            Console.WriteLine("Enter delimiter (if ignored, comma is used)");
            string firstDelimiter = Console.ReadLine();
            if (string.IsNullOrEmpty(firstDelimiter))
            {
                firstDelimiter = ",";
            }

            Console.WriteLine("Enter second delimiter (optional). If your data contains characters like the first delimiter, use the second one");
            string secondDelimiter = Console.ReadLine();

            Console.WriteLine("Enter the comma separated header for the file:");
            string header = Console.ReadLine();
            if (string.IsNullOrEmpty(header))
            {
                Console.WriteLine("No data input to fetch");
                return;
            }
            Console.WriteLine("Do you need counts only on enumerables (Y/N)?");
            string answer = Console.ReadLine();
            if (answer == "Y" || answer == "y")
            {
                countsOnly = true;
            }
            string[] headerInOrder = header.Split(',');
            Dictionary<string, List<int>> headerToIndexesMap = new Dictionary<string, List<int>>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < headerInOrder.Length; i++)
            {
                if (!headerToIndexesMap.ContainsKey(headerInOrder[i]))
                {
                    headerToIndexesMap[headerInOrder[i]] = new List<int>(){i};
                }
                else
                {
                    headerToIndexesMap[headerInOrder[i]].Add(i);
                }
            }

            StringBuilder sb = new StringBuilder();
            sb.AppendLine(string.Join(firstDelimiter, headerInOrder));
            foreach (string fullName in Directory.GetFiles(jsonDirectoryPath, "*.json"))
            {
                try
                {
                    BsonDocument document = BsonDocument.Parse(File.ReadAllText(fullName));
                    var sorted = new SortedDictionary<int, string>();
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
            }

            File.WriteAllText("results.txt", sb.ToString());
            Console.WriteLine();
            Console.WriteLine("Results are written to results.txt");
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
}
