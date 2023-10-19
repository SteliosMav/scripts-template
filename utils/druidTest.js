const druidHelper = require("./DruidHelper");
const druid = new druidHelper();
const moment = require("moment");

global.Parse = require("parse/node").Parse;
Parse.initialize(
    "Q8p0cZi0Es6POXNb4tNqqP7NdzXsqKd9Mzzdkdq6",
    "CPB9fwdT8l9KrLG55a4oSCf8XpewaJMX75nJk3kr",
    "I4duQVcXNFqJUb8YpmDWEVgArtWwN5otJIgFkE3d"
);
Parse.serverURL = "https://data-jobs.instashop.ae/parse"; //Production
//Parse.serverURL = "https://data-dev.instashop.ae/parse"; //Development
//Parse.serverURL = "https://data-staging.instashop.ae/parse"; //Staging
//Parse.serverURL = "http://localhost:5000/parse";
global.sessionToken = "r:5ab11c564195e08a2869264e9c412508";

const MongoClient = require("mongodb").MongoClient;
//const url ="mongodb+srv://christodouloslemperos:gQhXEDA2MBHQrfMWJsNPyDFChBESZe2hw29fCfYSQE7t53js@instadata-prod.cq0yu.mongodb.net/instadata?retryWrites=true&readPreference=secondary&readPreferenceTags=nodeType:ANALYTICS&w=majority";
const url = "mongodb+srv://christodouloslemperos:gQhXEDA2MBHQrfMWJsNPyDFChBESZe2hw29fCfYSQE7t53js@instadata-dev.mwztz.mongodb.net/instadata?retryWrites=true&w=majority"

const client = new MongoClient(url, {
    useUnifiedTopology: true,
});

function getIdFromPointer(value) {
    return value && value.split("$")[1] || undefined;
}

async function f() {
    console.time('test');

    const query2 = {
        query: `SELECT  orderId,
                        __time,
                        clientId,
                        orderCode,
                        retailPriceAED,
                        numberOfAllProducts,
                        removedItems,
                        status,
                        reason
                    FROM OrdersSales 
                    where status !='Completed' 
                    and status !='Deleted' 
                    and testClient is null
                    and __time >='2022-06-01' 
                    AND companyId = 'DnSeKcd563' 
                    AND reason in ('gbLCYsTcqb', 'dd4x20jdBp')`
    }
    const results = await druid.fetchResultsSQL(query2);
    const orderIds = results.map(el => el.orderId);

    try {
        await client.connect();
        let db = await client.db();

        const clients = await new Parse.Query("Clients")
            .equalTo("company", "Companies$DnSeKcd563")
            .select("_id")
            .find({sessionToken})
        const clientIds = clients.map(el => {el._id})
        const orders = await new Parse.Query("Orders")
            .containedIn("objectId", orderIds)
            .select("numericId", "client.name", "reason.name", "finalPrice")
            .limit(orderIds.length)
            .find({sessionToken});
        const orderItems = await new Parse.Query("OrderItem")
            .containedIn("order", orderIds)
            .doesNotExist("substituteFor")
            .select("quantity", "product.title", "product.barcode", "price", "client", "order", "status")
            .limit(orderIds.length * 15)
            .find({sessionToken});
        const productIds = orderItems.map(orderItem => orderItem.get("product").id);
        const productObjects = await new Parse.Query("ProductObject")
            .containedIn("productId", productIds)
            .containedIn("clientId", clientIds)
            .select("plu", "productId", "clientId")
            .limit(productIds.length * clientIds.length)
            .find({sessionToken});

        const data = [];
        for (const order of orders) {
            const items = orderItems.filter(orderItem => orderItem.get("order") === order.id);
            for (const item of items) {
                const product = item.get("product");
                // const productObject = productObjects.find(productObject => {
                //     return productObject.get("productId") === product.id && order.get("client")?.id === productObject.get("clientId")
                // });
                data.push({
                    Orderdate: moment(order.get("createdAt")).format("DD/MM/YYYY hh:mm"),
                    Ordercode: order.get("numericId")?.toString(),
                    Storename: order.get("client")?.get("name") || "",
                    Cancellationreason: order.get("reason")?.[0]?.get("name") || "",
                    Productname: product.get("title") || "",
                    Productstatus: item.get("status") || "",
                    FinalPrice: order.get("finalPrice")?.toString(),
                    Primarybarcode: product.get("barcode")?.value || "",
                });
            }
        }
        // const ordersCollection = db.collection("Orders");
        // const ordersItemCollection = db.collection("OrderItem");
        // const productsCollection = db.collection("Products");
        // const clientsCollection = db.collection("Clients");
        // const cancelationReasonCollection = db.collection("CancelationReason");

        // const canceledOrders = await ordersItemCollection.find({
        //     order: {$in: orderIds}
        // })//.project({_id: 1, _created_at: 1, _p_client: 1, numericId: 1, finalPrice: 1, reason: 1, ['substitutes.substitutes']: 1})
        // .toArray()

        // const data = canceledOrders.reduce((acc, cur) => {
        //     if(!acc[cur['order']]) acc[cur['order']] = [];
        //     acc[cur['order']].push(cur);
        //     return acc;
        // }, {})

        // let final_data = [];
        // for(key in data){
        //     const orderNumericId = await ordersCollection.find({_id: key}).project({numericId: 1, _created_at:1, finalPrice: 1, reason:1}).toArray();
        //     const reason = await cancelationReasonCollection.find({_id: orderNumericId[0].reason[0].objectId}).project({name: 1}).toArray()
        //     for(let i = 0; i < data[key].length; i++){
        //         const client = await clientsCollection.find({_id: data[key][i].client}).project({name: 1}).toArray();
        //         const product = await productsCollection.find({_id: getIdFromPointer(data[key][i]._p_product)}).project({'barcode.value': 1}).toArray();
        //         final_data.push({
        //             orderNumericId: orderNumericId[0].numericId,
        //             orderDate: orderNumericId[0]._created_at,
        //             client: client[0].name,
        //             totalValue: orderNumericId[0].finalPrice,
        //             product: product[0].barcode?.value || orderNumericId[0]._id,
        //             productStatus: data[key].status,
        //             reason: reason[0].name,
        //         })
        //     } 
        // }

        const xl = require("excel4node");
        const wb = new xl.Workbook();
        const ws = wb.addWorksheet("past6monthsCanceledOrders");
        const style = wb.createStyle({
            font: {
                bold: true
            }
        });
        ws.cell(1, 1).string("Order date").style(style);
        ws.cell(1, 2).string("Order code").style(style);
        ws.cell(1, 3).string("Store name").style(style);
        ws.cell(1, 4).string("Cancellation reason").style(style);
        ws.cell(1, 5).string("Product name").style(style);
        ws.cell(1, 6).string("Product status").style(style);
        ws.cell(1, 7).string("Final Price").style(style);
        ws.cell(1, 8).string("Primary barcode").style(style);
        let i = 2;
        for (const entry of data) {
            ws.cell(i, 1).string(entry.Orderdate);
            ws.cell(i, 2).string(entry.Ordercode);
            ws.cell(i, 3).string(entry.Storename);
            ws.cell(i, 4).string(entry.Cancellationreason);
            ws.cell(i, 5).string(entry.Productname);
            ws.cell(i, 6).string(entry.Productstatus);
            ws.cell(i, 7).string(entry.FinalPrice);
            ws.cell(i, 8).string(entry.Primarybarcode);
            i++;
        }
        wb.write("./excelFiles/past6monthsCanceledOrders.xlsx");

        console.log('finished');
        console.timeEnd('test');
            


    } catch (error) {
        console.log(error);
    }

    
    
}

f().then();

// SELECT userId
// FROM ProductsSales
// WHERE __time >='2022-03-09' AND orderStatus='Completed'
//     AND countryId = 'vrii93Zwoj'
//     AND categoryId='BgyfzEbp2Y'
//     AND clientBusinessType='grocery'
//     AND productStatus IN ('packaged','substituted')
// GROUP BY userId


// SELECT userId, SUM(1) as "quantity"
// FROM OrdersSales
// WHERE __time >='2022-03-09' AND status='Completed' AND countryId = 'vrii93Zwoj'
// GROUP BY userId
