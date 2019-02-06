const AWS = require('aws-sdk');
const dynamodb = new AWS.DynamoDB({ region: 'us-east-1', apiVersion: '2012-08-10' });

exports.handler = (event, context, callback) => {

    if (event.Records[0].eventName === 'INSERT') {

        console.log('Order details: ', event);
        console.log('eventName', event.Records[0].eventName);
        console.log('object', event.Records[0].dynamodb);
        console.log('price', event.Records[0].dynamodb.NewImage.price.N);
        console.log('minProductRating', event.Records[0].dynamodb.NewImage.minProductRating.N);
        console.log('bestOfferType', event.Records[0].dynamodb.NewImage.bestOfferType.S);
        console.log('productID', event.Records[0].dynamodb.NewImage.productID.S);

        const productIDFromUser = event.Records[0].dynamodb.NewImage.productID.S;
        const priceFromUser = event.Records[0].dynamodb.NewImage.price.N;

        const params = {
            TableName: 'OfferTable',
            IndexName: "productID-price-index",
            KeyConditionExpression: "productID = :a and price < :p",
            ExpressionAttributeValues: {
                ":a": {
                    S: productIDFromUser
                },
                ":p": {
                    N: priceFromUser
                }
            },
            ProjectionExpression: "companyID, offerID, available, price, productID",
            ScanIndexForward: false
        };

        dynamodb.query(params, function (err, data) {
            if (err) {
                console.log(err);
                callback(err);
            } else {
                console.log('offer data = ', data);
                console.log('price offer ', data.Items[0].price.N, )
                console.log('available offer ', data.Items[0].available.N, )
                console.log('productID offer ', data.Items[0].productID.S, )
            }
        });

    }
};
