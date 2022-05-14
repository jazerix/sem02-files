const server = require('ws');
const wss = new server.Server({port:5090})
const { MongoClient } = require('mongodb');

const url = 'mongodb://localhost:27017';
const client = new MongoClient(url);
const dbName = 'iot';

let recognizedClients = [];

wss.getUniqueID = function () {
    function s4() {
        return Math.floor((1 + Math.random()) * 0x10000).toString(16).substring(1);
    }
    return s4() + s4() + s4();
};

async function main() {
    await client.connect();
    console.log('Connected successfully to database');
    const db = client.db(dbName);

    const collection = db.collection('data');
    wss.on("connection", connection => {
        connection.id = wss.getUniqueID();
        connection.authed = false;
        connection.name = null;12
        console.log("client connected");
    
        connection.on("message", data => {
            try
            {
                let json = JSON.parse(data.toString());
                if ("type" in json === false)
                {
                    console.log("Missing type!");
                    connection.send("Unable to parse your message, missing type key");
                    return;
                }
    
                let type = json.type;
    
                if (type === "announce")
                    return upgradeClient(connection, json);
    
                if (!connection.authed)
                {
                    connection.send("Not authenticated");
                    return;
                }
    
                if (type === "clients")
                {
                    return listClients(connection);
                }
    
                if (type === "data")
                {
                    return logData(connection, json, collection);
                }
                
            }
            catch
            {
                console.log("Incorrectly formatted message received");
            }
        })

        connection.on("close", () => {
            recognizedClients = recognizedClients.filter(x => x.id !== connection.id);

            console.log("client disconnected");
        })
    })
    
    console.log("Server running on port 5090");
}

main()


function upgradeClient(connection, announce)
{
    if (connection.authed === true)
    {
        connection.send("already authed");
        return;
    }

    if ("name" in announce === false)
    {
        connection.send("Missing your name");
        return;
    }

    connection.name = announce.name;
    connection.authed = true;
    recognizedClients.push(connection);

    connection.send("Hello " + connection.name);
}

function listClients(connection)
{
    connection.send(JSON.stringify(recognizedClients.map(x => {
        return {
            id: x.id,
            name: x.name
        }
    })))
}

function logData(connection, json, collection)
{
    if ("data" in json === false)
    {
        connection.send("Missing data!");
        return;
    }

    collection.insertOne({

        client_id: connection.id,
        client_name: connection.name,
        timestamp: new Date(),
        data: json.data
    })
}