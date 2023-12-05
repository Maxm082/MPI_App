//подключение к БД происходит в строке 20, в строке 165
using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;
using Microsoft.EntityFrameworkCore;
using MPI;
using MySql.Data.MySqlClient;
using System.Diagnostics;
using Newtonsoft.Json;

public class SampleDbContext : DbContext
{
    public DbSet<TableDbPhoneIp> db_phone_ip { get; set; }
    public DbSet<TableGames> games { get; set; }
    public DbSet<TableLineups> lineups { get; set; }

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
public class NestedResult
{
    public TableDbPhoneIp t1 { get; set; }
    public TableDbPhoneIp t2 { get; set; }
}
[Table("GameLineupsResults")]
public class GameLineupsResults
{
    public TableGames Game { get; set; }
    public TableLineups Lineup { get; set; }
}
[Table("JoinedResults")]
public class JoinedResults
{
    public TableDbPhoneIp DbPhoneIp { get; set; }
    public TableGames Game { get; set; }
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
                result = dbContext.games
                        .Join(dbContext.lineups, game => game.game_id, lineup => lineup.game_id, (game, lineup) => new { game, lineup })
                        .Skip(start)
                        .Take(chunkSize);
                return JsonConvert.SerializeObject(result);
            case 7:
                result = dbContext.db_phone_ip
                    .GroupJoin(
                        dbContext.games,
                        db_phone_ip => db_phone_ip.id,
                        game => game.game_id,
                        (db_phone_ip, games) => new { DbPhoneIp = db_phone_ip, Games = games.DefaultIfEmpty() })
                    .SelectMany(x => x.Games, (db_phone_ip, game) => new { DbPhoneIp = db_phone_ip.DbPhoneIp, Game = game })
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
                List<TableDbPhoneIp> list1 = JsonConvert.DeserializeObject<List<TableDbPhoneIp>>(gatheredResults[0]);
                foreach (var item in list1)
                {
                    Console.WriteLine($"\n {item.id} " +
                        $"{item.email} {item.country} {item.city} {item.full_name} {item.phone} {item.ip} \n");
                }
                break;
            case 2:
                List<TableDbPhoneIp> list2 = JsonConvert.DeserializeObject<List<TableDbPhoneIp>>(gatheredResults[0]);
                foreach (var item in list2)
                {
                    Console.WriteLine($"\n {item.id} " +
                        $"{item.email} {item.country} {item.city} {item.full_name} {item.phone} {item.ip} \n");
                }
                break;
            case 3:
                List<TableDbPhoneIp> list3 = JsonConvert.DeserializeObject<List<TableDbPhoneIp>>(gatheredResults[0]);
                foreach (var item in list3)
                {
                    Console.WriteLine($"\n {item.id} " +
                        $"{item.email} {item.country} {item.city} {item.full_name} {item.phone} {item.ip} \n");
                }
                break;
            case 4:
                List<TableDbPhoneIp> list4 = JsonConvert.DeserializeObject<List<TableDbPhoneIp>>(gatheredResults[0]);
                foreach (var item in list4)
                {
                    Console.WriteLine($"\n {item.id} " +
                        $"{item.email} {item.country} {item.city} {item.full_name} {item.phone} {item.ip} \n");
                }
                break;
            case 5:
                List<NestedResult> nestedResults5 = JsonConvert.DeserializeObject<List<NestedResult>>(gatheredResults[0]);
                foreach (var nestedResult in nestedResults5)
                {
                    Console.WriteLine($"\n table1 " +
                        $"{nestedResult.t1.id} {nestedResult.t1.email}" +
                        $" {nestedResult.t1.country} {nestedResult.t1.city}" +
                        $" {nestedResult.t1.full_name} {nestedResult.t1.phone}" +
                        $" {nestedResult.t1.ip} \n {nestedResult.t2.id} " +
                        $"{nestedResult.t2.email} {nestedResult.t2.country} {nestedResult.t2.city} " +
                        $"{nestedResult.t2.full_name} {nestedResult.t2.phone} {nestedResult.t2.ip} \n");
                }
                break;
            case 6:
                List<GameLineupsResults> gameLineupsResults = JsonConvert.DeserializeObject<List<GameLineupsResults>>(gatheredResults[0]);
                foreach (var result in gameLineupsResults)
                {
                    Console.WriteLine($"Game Info: {result.Game.game_id} {result.Game.team} {result.Game.city} {result.Game.goals} {result.Game.own}");
                    Console.WriteLine($"Lineup Info: {result.Lineup.game_id} {result.Lineup.player_id} {result.Lineup.start}" +
                        $" {result.Lineup.cards} {result.Lineup.time_in} {result.Lineup.goals} \n");
                }
                break;
            case 7:
                List<JoinedResults> joinedResults = JsonConvert.DeserializeObject<List<JoinedResults>>(gatheredResults[0]);
                foreach (var result in joinedResults)
                {
                    Console.WriteLine($"Информация о DbPhoneIp: {result.DbPhoneIp.id} {result.DbPhoneIp.email} {result.DbPhoneIp.country} {result.DbPhoneIp.city} {result.DbPhoneIp.full_name} {result.DbPhoneIp.phone} {result.DbPhoneIp.ip} ");
                    Console.WriteLine($"Информация об игре: {result.Game.game_id} {result.Game.team} {result.Game.city} {result.Game.goals} {result.Game.own} \n");
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

