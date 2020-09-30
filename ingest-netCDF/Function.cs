using System.Threading.Tasks;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Newtonsoft.Json.Linq;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]
//[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace ingest_netCDF
{
    public class Function
    {
        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an SQS event object and can be used 
        /// to respond to SQS messages.
        /// </summary>
        public async Task FunctionHandler(SQSEvent evnt, ILambdaContext context)
        //public async Task FunctionHandler(MetOfficeSQSEvent evnt, ILambdaContext context)
        {
            foreach (var message in evnt.Records)
            {
                await ProcessMessageAsync(message, context);
            }
        }

        /// <summary>
        /// Copy the new S3 file from the Met Office bucket into our ingest bucket
        /// </summary>
        private async Task ProcessMessageAsync(SQSEvent.SQSMessage sqsMessage, ILambdaContext context)
        {
            // First, extract the original message we received from SNS, by unwrapping it from the sqs Message headers
            JObject snsMessage = JObject.Parse(sqsMessage.Body);

            // Now get the file notification details from the "Message" field of the SNS message
            JObject fileNotification = JObject.Parse(snsMessage["Message"].ToString());

            string bucket = fileNotification["bucket"].ToString();
            string key = fileNotification["key"].ToString();
            context.Logger.LogLine($"Received SQS notification new file available: s3://{bucket}/{key}");

            AmazonS3Client s3 = new AmazonS3Client();

            CopyObjectResponse result = await s3.CopyObjectAsync(bucket, key, "bigwind-ingest", key);
            if ((((int)result.HttpStatusCode) < 200) || (((int)result.HttpStatusCode) > 299))
            {
                throw new System.Exception($"S3 copy request failed with HttpStatusCode: {result.HttpStatusCode}");
            }
        }

        /* Sample JSON document to use when testing this Lambda function
{
    "Records": [
            {
            "messageId": "fe711259-d2fa-46a5-a02f-c97286449efa",
            "receiptHandle": "AQEBQiU4JggNx0JbYFQ6PSet7JxWOvlu61MHPPTSWWreu8u7MRVGd78CC70PIiPRf/gLJ5ALvFSJSzo2Dvh75V17SjFY3FqVjw9yTl2Igo/yLlnqVK2BhnTCDdfwNcb1+r9xzoTIjfpK2Xq5qvb2qRMtvBftat78Zpjn2u/xgxnBqpTgYwsnn/HVJkUr9fczr2LBvkFgVwwGutMxHgFmMsqrJnycTTTVKlpxtyfCObs9Y0rJvnKrLVQkE8LewERMwVb638U1IJl24TBRwuBJGNie3CyGmlVVpcwmnoXR6bAGqTnybQkKp+XRYBJebkKTfZsPzFwCpSizggBGtz2Sv6uc9c+lUoZYZR600/epDcee8nf+IUYHmJ24wg/6Iq/GnAZaKuhsnYyj+QxF4mTBYJ5UQQ==",
            "body": "{\n  \"Type\" : \"Notification\",\n  \"MessageId\" : \"7c8414f4-dad4-5ebc-b1e0-431aca50ffd4\",\n  \"TopicArn\" : \"arn:aws:sns:eu-west-2:021908831235:aws-earth-mo-atmospheric-ukv-prd\",\n  \"Message\" : \"{\\\"model\\\": \\\"mo-atmospheric-ukv-prd\\\", \\\"height_units\\\": \\\"m\\\", \\\"ttl\\\": 1602046145, \\\"time\\\": \\\"2020-10-01T05:00:00Z\\\", \\\"bucket\\\": \\\"aws-earth-mo-atmospheric-ukv-prd\\\", \\\"created_time\\\": \\\"2020-09-29T04:34:45Z\\\", \\\"name\\\": \\\"air_temperature\\\", \\\"object_size\\\": 1617556.0, \\\"forecast_period\\\": \\\"180000\\\", \\\"forecast_reference_time\\\": \\\"2020-09-29T03:00:00Z\\\", \\\"forecast_period_units\\\": \\\"seconds\\\", \\\"height\\\": \\\"1.5\\\", \\\"key\\\": \\\"78711792d75acbe108710d159b2eb582209562b1.nc\\\"}\",\n  \"Timestamp\" : \"2020-09-30T04:49:08.768Z\",\n  \"SignatureVersion\" : \"1\",\n  \"Signature\" : \"UbfGnLH1BKAPXU2VkWkH8leGcSsjS39U6OUNpbSUeGuD/7DDG1LA8X2cX/i58GDt/ETTsrviO06AfieCfoQAbj3AyNwdwwaTR+qv3cLMv0ormgHnMoEm1imbFcNgigvn5glUPbtT+lpJ4Osu414ftTJkvnB8Dd45MdInDtdWr27cM9oObv7pVDRBe8um8rGQyz5Vdy4Bj1XMLov4OnT79wOjhy4iMpRhwBQIe+ROYUURXLY3OJf/v5yK9+6i3iVtpk/r9wxQGEcjijwY+3NZ2fhXniR5xjsKC58M+sgD5yLNNpPET1AQmH1lxyo41rdXoF7LhXlkpS/WNQ9mk15C5A==\",\n  \"SigningCertURL\" : \"https://sns.eu-west-2.amazonaws.com/SimpleNotificationService-a86cb10b4e1f29c941702d737128f7b6.pem\",\n  \"UnsubscribeURL\" : \"https://sns.eu-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-west-2:021908831235:aws-earth-mo-atmospheric-ukv-prd:31924bae-cdf0-4530-8f67-639ebd716da6\",\n  \"MessageAttributes\" : {\n    \"forecast_reference_time\" : {\"Type\":\"String\",\"Value\":\"2020-09-29T03:00:00Z\"},\n    \"name\" : {\"Type\":\"String\",\"Value\":\"air_temperature\"},\n    \"model\" : {\"Type\":\"String\",\"Value\":\"mo-atmospheric-ukv-prd\"}\n  }\n}",
            "attributes": {
                "ApproximateReceiveCount": "94",
                "SentTimestamp": "1601441349083",
                "SenderId": "AIDAIVEA3AGEU7NF6DRAG",
                "ApproximateFirstReceiveTimestamp": "1601441349083"
            },
            "messageAttributes": {},
            "md5OfBody": "34ab3a5e3a23cf218f7adeb8a8a6ff23",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:us-east-1:191899815322:bigwind-notifications",
            "awsRegion": "us-east-1"
        }
    ]
}
*/
        public Function() { }
    }
}
