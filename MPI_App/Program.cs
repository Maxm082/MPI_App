//подключение к БД происходит в строке 21, в строке 214
using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;
using Microsoft.EntityFrameworkCore;
using MPI;
using MySql.Data.MySqlClient;
using System.Diagnostics;
using Newtonsoft.Json;
using MySqlX.XDevAPI.Common;

public class SampleDbContext : DbContext
{
    public DbSet<TableDbPhoneIp> db_phone_ip { get; set; }
    public DbSet<TableGames> games { get; set; }
    public DbSet<TableLineups> lineups { get; set; }
    public DbSet<TableResults> results { get; set; }


    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        string connectionString = "Server=localhost;Database=mydb;User ID=root;Password=%qwerty12345;";
        optionsBuilder.UseMySql(connectionString, new MySqlServerVersion(new Version(8, 2, 00)));
    }
}

[Table("db_phone_ip")]
public class TableDbPhoneIp
{
    [Key]
    public int id { get; set; }
    public string email { get; set; }
    public string country { get; set; }
    public string city { get; set; }
    public string full_name { get; set; }
    public string phone { get; set; }
    public string ip { get; set; }
}

[Table("games")]
public class TableGames
{
    [Key]
    public int game_id { get; set; }
    public string team { get; set; }
    public string city { get; set; }
    public int goals { get; set; }
    public int own { get; set; }
}
[Table("lineups")]
public class TableLineups
{
    [Key]
    public int game_id { get; set; }
    public int player_id { get; set; }
    public char start { get; set; }
    public char? cards { get; set; }
    public int? time_in { get; set; }
    public int? goals { get; set; }
}
[Table("results")]
public class TableResults
{
    public int id { get; set; }
    public string email { get; set; }
    public string country { get; set; }
    public string city1 { get; set; }
    public string full_name { get; set; }
    public string phone { get; set; }
    public string ip { get; set; }
    public int game_id1 { get; set; }
    public string team { get; set; }
    public string city2 { get; set; }
    public int goals1 { get; set; }
    public int own { get; set; }
    public int game_id2 { get; set; }
    public int player_id { get; set; }
    public char start { get; set; }
    public char? cards { get; set; }
    public int? time_in { get; set; }
    public int? goals2 { get; set; }
}

    class Program
{
    static string ExecuteQuery(SampleDbContext dbContext, int selectedQuery, int start, int chunkSize)
    {
        IQueryable<object> result;
        switch (selectedQuery)
        {
            case 1:
                result = dbContext.db_phone_ip
                        .Where(entity => entity.id == 90000)
                        .Skip(start).Take(chunkSize);
                return JsonConvert.SerializeObject(result);
            case 2:
                result = dbContext.db_phone_ip
                        .Where(entity => entity.country == "Russia")
                        .Skip(start).Take(chunkSize);
                return JsonConvert.SerializeObject(result);
            case 3:
                result = dbContext.db_phone_ip
                        .Where(entity => entity.email.Contains("garry.me"))
                        .Skip(start).Take(chunkSize);
                return JsonConvert.SerializeObject(result);
            case 4:
                result = dbContext.db_phone_ip
                        .Where(entity => entity.email.Contains(".io"))
                        .Skip(start).Take(chunkSize);
                return JsonConvert.SerializeObject(result);
            case 5:
                result = dbContext.db_phone_ip
                        .Join(dbContext.db_phone_ip, t1 => true, t2 => true, (t1, t2) => new { t1, t2 })
                        .Skip(start).Take(chunkSize);
                return JsonConvert.SerializeObject(result);
            case 6:
                result = dbContext.db_phone_ip
                        .Join(dbContext.games, game => game.id, db_phone_ip => db_phone_ip.game_id, (game, db_phone_ip) => new { game, db_phone_ip })
                        .Skip(start).Take(chunkSize);
                return JsonConvert.SerializeObject(result);
            case 7:
                result = dbContext.games
                        .Join(dbContext.lineups, game => game.game_id, lineup => lineup.game_id, (game, lineup) => new { game, lineup })
                        .Skip(start)
                        .Take(chunkSize);
                return JsonConvert.SerializeObject(result);
            default:
                result = dbContext.db_phone_ip
                        .Skip(start).Take(chunkSize);
                return JsonConvert.SerializeObject(result);
        }
    }
    static void PrintGatheredResults(string[] gatheredResults, int selectedQuery)
    {
        switch (selectedQuery)
        {
            case 1:
                foreach (string strGatheredResults in gatheredResults)
                {
                    TableDbPhoneIp objStrGathere = JsonConvert.DeserializeObject<TableDbPhoneIp>(strGatheredResults);
                    Console.WriteLine($"{objStrGathere.id}");
                }
                break;
            case 2:
                foreach (string StrGatheredResults in gatheredResults)
                {
                    Console.WriteLine(JsonConvert.DeserializeObject<object>(StrGatheredResults));
                }
                break;
            case 3:
                foreach (string StrGatheredResults in gatheredResults)
                {
                    Console.WriteLine(JsonConvert.DeserializeObject<object>(StrGatheredResults));
                }
                break;
            case 4:
                foreach (string StrGatheredResults in gatheredResults)
                {
                    Console.WriteLine(JsonConvert.DeserializeObject<object>(StrGatheredResults));
                }
                break;
            case 5:
                foreach (string StrGatheredResults in gatheredResults)
                {
                    Console.WriteLine(JsonConvert.DeserializeObject<object>(StrGatheredResults));
                }
                break;
            case 6:
                foreach (string StrGatheredResults in gatheredResults)
                {
                    Console.WriteLine(JsonConvert.DeserializeObject<object>(StrGatheredResults));
                }
                break;
            case 7:
                foreach (string StrGatheredResults in gatheredResults)
                {
                    Console.WriteLine(JsonConvert.DeserializeObject<object>(StrGatheredResults));
                }
                break;
            default:
                foreach (string StrGatheredResults in gatheredResults)
                {
                    Console.WriteLine(JsonConvert.DeserializeObject<object>(StrGatheredResults));
                }
                break;
        }
    }
    static int GetDataSize(MySqlConnection connection, string table)
    {
        using (MySqlCommand command = new MySqlCommand($"SELECT count(*) FROM {table}", connection))
        {
            return (int)(long)command.ExecuteScalar();
        }
    }


    static void Main(string[] args)
    {
        Stopwatch stopwatch = new Stopwatch();

        using (new MPI.Environment(ref args))
        {
            Intracommunicator comm = MPI.Communicator.world;
            int selectedQuery = 0;

            if (comm.Rank == 0)
            {
                Console.WriteLine("Выберите SQL-запрос (1-7): ");
                selectedQuery = int.Parse(Console.ReadLine());
                stopwatch.Start();
            }

            comm.Broadcast(ref selectedQuery, 0);

            string connectionString = "Server=localhost;Database=mydb;User ID=root;Password=%qwerty12345;";
            MySqlConnection connection = new MySqlConnection(connectionString);
            connection.Open();

            double dataSize = GetDataSize(connection, (selectedQuery == 7) ? "games" : "db_phone_ip");
            int chunkSize = (int)(Math.Ceiling(dataSize / comm.Size));
            int start = comm.Rank * chunkSize;

            using (var dbContext = new SampleDbContext())
            {
                string[] gatheredResults = comm.Gather(ExecuteQuery(dbContext, selectedQuery, start, chunkSize), 0); //Передача в 0 поток информации. gatheredResults хранит массив строк

                if (comm.Rank == 0)
                {
                    PrintGatheredResults(gatheredResults, selectedQuery);
                    stopwatch.Stop();
                    Console.WriteLine($"Программа выполнялась {stopwatch.ElapsedMilliseconds} миллисекунд.");
                }
            }
            connection.Close();
        }
    }
}

