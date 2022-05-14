using MongoDB.Bson;
using MongoDB.Driver;

var client = new MongoClient("mongodb://localhost:27017");

var database = client.GetDatabase("iot");
var data = database.GetCollection<BsonDocument>("data");

var recordings = data.Find(Builders<BsonDocument>.Filter.Eq("client_name", "SDU Listener")).ToList();

if (recordings.Count == 0)
    return;

var firstTime = (recordings.First().Values.ToList().ElementAt(0) as BsonDateTime)!.ToUniversalTime().ToLocalTime();

var groupings = new Dictionary<string, double>();

var delta = firstTime.Ticks % TimeSpan.FromSeconds(5).Ticks;
var startTime = new DateTime(firstTime.Ticks - delta, firstTime.Kind);
var endTime = startTime.AddSeconds(5);

var currentInterval = new List<long>();

foreach (var recording in recordings.ToList())
{
    var fields = recording.Values.ToList();

    var recordedAt = (fields.ElementAt(0) as BsonDateTime)!.ToUniversalTime().ToLocalTime();
    if (recordedAt > endTime)
    {
        // average and dump data
        var average = currentInterval.Average();
        groupings.Add(startTime.ToString("G") + "-" + endTime.ToString("T"), Math.Floor(average));
        currentInterval.Clear();

        // advance pointer
        startTime = startTime.AddSeconds(5);
        endTime = startTime.AddSeconds(5);
    }

    var frequencies = ((fields.First(x => x is BsonDocument) as BsonDocument)!.ToList().First().Value as BsonArray)?.Values;
    var maxVal = (from BsonDocument? frequency in frequencies! select frequency.Values.ElementAt(1).ToInt64()).Max();

    currentInterval.Add(maxVal);
}


// group into 5 second intervals
File.WriteAllText("out2.csv", string.Join(Environment.NewLine, groupings.Select(d => $"{d.Key},{d.Value}")));