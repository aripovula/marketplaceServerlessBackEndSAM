const AWS = require('aws-sdk');
const axios = require('node_modules/axios/lib/axios.js');

exports.handler = async (event) => {

    AWS.config.update({
        region: 'us-east-1',
        credentials: new AWS.Credentials({
            accessKeyId: "abc123",
            secretAccessKey: "abc123",
        })
    });
    const result = await invokeAppSync({
        text1: 'New6',
        text2: 'LD15'
    });

    console.log(result);
    return result.data;
};

const invokeAppSync = async ({
    text1,
    text2
}) => {
    let req = new AWS.HttpRequest('https://bac.appsync-api.us-east-1.amazonaws.com/graphql', 'us-east-1');
    req.method = 'POST';
    req.headers.host = 'bac.appsync-api.us-east-1.amazonaws.com';
    req.headers['Content-Type'] = 'multipart/form-data';
    req.body = JSON.stringify({
        "query": "mutation ($input: CreateProductInput!) { createProduct(input: $input){  id, name, modelNo, specificationURL, imageURL, lastTenRatingAverage } }",
        "variables": {
            "input": {
                "name": text1,
                "modelNo": text2,
                specificationURL: "String2",
                imageURL: "String2",
                lastTenRatingAverage: 2.9
            }
        }
    });
    let signer = new AWS.Signers.V4(req, 'appsync', true);
    signer.addAuthorization(AWS.config.credentials, AWS.util.date.getDate());
    const result = await axios({
        method: 'post',
        url: 'https://bac.appsync-api.us-east-1.amazonaws.com/graphql',
        data: req.body,
        headers: req.headers
    });
    return result;
};