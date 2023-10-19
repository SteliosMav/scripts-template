const druidHelper = require("./DruidHelper");
const druid = new druidHelper();
const moment = require("moment");
const xl = require("excel4node");
const readXlsxFile = require('read-excel-file/node')
require('dotenv').config();

global.Parse = require("parse/node").Parse;
Parse.initialize(
    "Q8p0cZi0Es6POXNb4tNqqP7NdzXsqKd9Mzzdkdq6",
    "CPB9fwdT8l9KrLG55a4oSCf8XpewaJMX75nJk3kr",
    "I4duQVcXNFqJUb8YpmDWEVgArtWwN5otJIgFkE3d"
);
//Parse.serverURL = "https://data-jobs.instashop.ae/parse"; //Production
//Parse.serverURL = "https://data-dev.instashop.ae/parse"; //Development
//Parse.serverURL = "https://data-staging.instashop.ae/parse"; //Staging
//Parse.serverURL = "http://localhost:5000/parse";
global.sessionToken = "r:5ab11c564195e08a2869264e9c412508";

const MongoClient = require("mongodb").MongoClient;

const client = new MongoClient(process.env.DB_URI, {
    useUnifiedTopology: true,
});

function getIdFromPointer(value) {
    return value && value.split("$")[1] || undefined;
}

(async () => {
    console.time('test');

    try {
        let fromDate = moment.utc("2023-10-17").startOf("day").toDate();
        let toDate = moment.utc("2023-10-23").endOf("day").toDate();

        await client.connect();
        let db = await client.db();
        const clientsCollection = db.collection("Clients");
        const ordersCollection = db.collection("Orders");
        const usersCollection = db.collection("_User");
        const deliveryDetailsCollection = db.collection("DeliveryDetails");

        // const clients = await clientsCollection.find({_p_country: "Countries$ryFmc6ACd1"}).project({_id: 1}).toArray();
        // const clientPointers = clients.map(c => "Clients$"+c._id);
        const orders = await ordersCollection
            .find({ status: "Completed", _created_at: {$gte: fromDate, $lte: toDate}, _p_country: "Countries$ryFmc6ACd1"})
            .project({_id: 1, comments: 1, _p_user: 1, retailPrice: 1, _p_deliveryDetails: 1})
            .toArray();

        const groupedOrders = orders.reduce((a,c)=>{
            if(c.comments.toLowerCase().includes("hyperound") ){
                if(!a[c._p_user]) a[c._p_user] = [];
                a[c._p_user].push(c);
            }
            return a
        },{})
        const data = [];
        for(const key in groupedOrders){
            const user = await usersCollection.findOne({_id: getIdFromPointer(key)});
            if(user.unverifiedEmail.toLowerCase().includes("@instashop.ae")) continue;
            const deliveryDetail = await deliveryDetailsCollection.findOne({_id: getIdFromPointer(groupedOrders[key][0]?._p_deliveryDetails)});
            data.push({
                ...groupedOrders[key][0],
                unverifiedEmail: user?.unverifiedEmail,
                address: deliveryDetail?.locationInfo?.lines?.join(" "),
                phoneNumber: deliveryDetail?.phoneNumber,
                name: deliveryDetail?.name
            });
        }

        const xl = require("excel4node");
        const wb = new xl.Workbook();
        const ws = wb.addWorksheet("hyperound");
        const style = wb.createStyle({
            font: {
                bold: true
            }
        });
        ws.cell(1, 1).string("Name").style(style);
        ws.cell(1, 2).string("User email").style(style);
        ws.cell(1, 3).string("User address").style(style);
        ws.cell(1, 4).string("User number").style(style);
        ws.cell(1, 5).string("Order amount").style(style);
        let i = 2;
        for (const entry of data) {
            ws.cell(i, 1).string(entry?.name?.toString() || "");
            ws.cell(i, 2).string(entry?.unverifiedEmail?.toString() || "");
            ws.cell(i, 3).string(entry?.address?.toString() || "");
            ws.cell(i, 4).string(entry?.phoneNumber?.toString() || "");
            ws.cell(i, 5).string(entry?.retailPrice?.toString() || "");
            i++;
        }
        wb.write("./excelFiles/hyperound.xlsx");

        console.log('finished');
        console.timeEnd('test');

    } catch (error) {
        console.log(error);
    }



})()
