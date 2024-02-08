import vief from './vief-utils.mjs';
import AWS from 'aws-sdk';

//DynamoDB Table 
const docClient = new AWS.DynamoDB.DocumentClient();
const tableName = 'extractor_table';

//S3Bucket 
const s3 = new AWS.S3();
const S3_ROOT_BUCKET = 'extractordocuments';

const updateExtractorInDDB = async (extractor) => {
    try {
        const params = {
            TableName: tableName,
            Item: extractor,
        };
        // Save the extractor to DynamoDB
        await docClient.put(params).promise();
    } catch (error) {
        return { statusCode: 500, error: 'Internal Server Error' };
    }
};

//Check if Extractor Exists using readExtractor(extId)
const extractorExists = async (extId) => {
    let res = await readExtractor(extId);
    return (200 <= res.statusCode < 300) ;
};

//  Write an extractor metadata to file system
const writeExtractor = async(extractor) => {
   console.log("ddb-extractor.mjs: Extractor Received: ", extractor);
   
   try {
        const params = {
            TableName: tableName,
            Item: extractor,
        };
        // Save the extractor to DynamoDB
        await docClient.put(params).promise();
        console.log("DynamoDB: ", extractor.extId);
        
        // S3 Bucket => Create a folder with extId as folder name
        const s3FolderKey = `extractors/${extractor.extId}/`;
        console.log("S3 Bucket: ", s3FolderKey);
        
        const s3Params = {
            Bucket: S3_ROOT_BUCKET,
            Key: s3FolderKey,
            Body: '' // Empty content for a folder
          // ACL: 'public-read' // Adjust the ACL based on your requirements
        };
      
      await s3.upload(s3Params).promise();
      return { statusCode: 201, body: `extId ${extractor.extId} successfully added to the table ${tableName} and S3 Bucket: ${S3_ROOT_BUCKET}` };
    } catch (error) {
        console.error('Error creating extId:', error.message || error);
        return { statusCode: 500, error: 'writeExtractor: Internal Server Error' };
    }
   
};

// GET for readAllExtractors
const readAllExtractors = async() => {
      console.log("ddb-extractor.mjs: readAllExtractors: Request Received"); 
      const params = {
        TableName: tableName,
      };
    
      try {
        const result = await docClient.scan(params).promise();
        console.log("ddb-extractor.mjs: result: ", result);
    
        if (result.Items && result.Items.length > 0) {
          return { statusCode: 200, body: result.Items };
        } else {
         return { statusCode: 404, error: `No data found in the table ${tableName}` };
        }
      } catch (error) {
        console.error('Error getting all data:', error);
        return { statusCode: 500, error: 'readAllExtractors: Internal Server Error' };
      }
};

// GET an extractor by ID
const readExtractor = async (extId) => {
    console.log('ddb-extractor.mjs: readExtractor: Request Received: extId:', extId);
    const params = {
        TableName: tableName,
        Key: {
            extId: extId,
        },
    };
    try {
        const result = await docClient.get(params).promise();

        if (result.Item) {
            return { statusCode: 200, body: result.Item };
        } else {
            return { statusCode: 404, error: `Extractor with ID ${extId} not found in the table ${tableName}` };
        }
    } catch (error) {
        console.error('Error getting extractor by ID:', error);
        return { statusCode: 500, error: 'readExtractor: Internal Server Error' };
    }
};

//PUT createDocument: Upload Documents to S3
const createDocument = async(extId, docType, documents) => {
    console.log('ddb-extractor.mjs: createExtractor: Request Received: createDocument');
    // Create a new doc Id, starting with character 'd' for RFP documents and 'c' for Census documents

    try {
        // Update the S3 key
        const s3KeyPrefix = `extractors/${extId}/${docType}/`;
        let uploadedDocs = [];

        // For Loop to Read Documents 
        for (const document of documents) {
            const s3Key = s3KeyPrefix + document.originalName;
            
            //Check if Document Exists?
            let documentExists=true;
            let version = 0;
            let s3DocumentPath = s3Key;
            
            // Find a unique name for the document
            while (documentExists) {
                // Check if the document exists in S3
                try {
                    await s3.headObject({ Bucket: S3_ROOT_BUCKET, Key: s3DocumentPath }).promise();
                    version++; // Increment version number
                    s3DocumentPath = `${s3KeyPrefix}${document.originalName.split('.')[0]}-copy(${version}).${document.originalName.split('.')[1]}`;
                } catch (error) {
                    documentExists = false; // Document doesn't exist, exit loop
                }
            }
            
            // Create a new doc Id, starting with character 'd' for RFP documents and 'c' for Census documents
            let docId = (docType == "RFP") ? vief.suuid(4) : vief.suuid(3);
            console.log("ddb-extractor.mjs: createDocument: docId: ", docId);
            
            // S3 Bucket: Upload document 
            const s3Params = {
                Bucket: S3_ROOT_BUCKET,
                Key: s3DocumentPath,
                Body: document.buffer
            };
            await s3.upload(s3Params).promise();

            // Update the 'docs' array with the S3 folder path
            const s3DocPath = `${s3DocumentPath}`;
            console.log(`Document uploaded to ${s3DocPath}`);

            uploadedDocs.push({ "docId": docId, "path": s3DocPath });
        }
        
        // Read the extractor from DynamoDB
        let result = await readExtractor(extId);
        let extractor = result.body;
            
        console.log("ddb-extractor.mjs: createExtractor: extractor: ", extractor);
        extractor.docs = [...extractor.docs, ...uploadedDocs];

        const updateParams = {
            TableName: tableName,
            Item: extractor,
        };
        await docClient.put(updateParams).promise();
        
        console.log("Uploaded docs: ", uploadedDocs);

        return { statusCode: 200, body: `Documents successfully uploaded to ${s3KeyPrefix}` };
    } catch (error) {
        console.error('Error uploading documents:', error.message || error);
        console.log(error);
        return { statusCode: 500, error: 'createDocument: Internal Server Error' };
    }
};

//GET readDocument: Read Document from S3
const readDocument = async(extId, docType, docId) => {
    console.log("ddb-extractor.mjs: readDocument: Request Received: ", extId, docType, docId);
    // Read the extractor from DynamoDB
    let result = await readExtractor(extId);
    let extractor = result.body;
    console.log("Extractor: ", extractor);
        
    try{
        let fdocs = extractor.docs.filter((doc) => doc.docId === docId);
        if(fdocs.length <= 0)
            return { statusCode: 404, error: 'Document not found' };
            
        // Extract the S3 path of the document
        const s3Path = fdocs[0].path;
        console.log("Document Path: ", s3Path);
        const s3Object = await s3.getObject({ Bucket: S3_ROOT_BUCKET, Key: s3Path }).promise();
        console.log("S3Object: ", s3Object);
        
        // Return the buffer stream
        const response =  {
            statusCode: 200,
            isBase64Encoded: true, // Indicate that the body is base64 encoded
            headers: {
                'Content-Type': 'application/octet-stream', 
                'Content-Disposition': `attachment; filename=${s3Path.split("/").pop()}`,
            },
            body: s3Object.Body.toString('base64'),
        };
        return response;
        
    } catch (error) {
        console.error('Error reading document:', error.message || error);
        return { statusCode: 500, error: 'readDocument: Internal Server Error' };
    }
};

//Delete a document with DocId
const deleteDocument = async(extId, docType, docId) => {
    console.log("ddb-extractor.mjs: deleteDocument: Request Received: ", extId, docType, docId);
    // Read the extractor from DynamoDB
    let result = await readExtractor(extId);
    let extractor = result.body;
    console.log("Extractor: ", extractor);
    
        try{
        let fdocs = extractor.docs.filter((doc) => doc.docId === docId);
        if(fdocs.length <= 0)
            return { statusCode: 404, error: 'Document not found' };
            
        // Extract the S3 path of the document
        const s3Path = fdocs[0].path;
        console.log("Document Path: ", s3Path);
        await s3.deleteObject({ Bucket: S3_ROOT_BUCKET, Key: s3Path }).promise(); 
        
        // Remove the document from the extractor's docs array
        extractor.docs = extractor.docs.filter((doc) => doc.docId != docId);
        
        // Update the extractor in DynamoDB
        const updateParams = {
            TableName: tableName,
            Item: extractor,
        };
        await docClient.put(updateParams).promise();


        return { statusCode: 200, body: `Document deleted from S3 Bucket` };
    } catch (error) {
        console.error('Error reading document:', error.message || error);
        return { statusCode: 500, error: 'deleteDocument: Internal Server Error' };
    }
};

//Delete an Extractor with extId
const deleteExtractor = async(extId) => {
    console.log("ddb-extractor.mjs: deleteDocument: Request Received: ", extId);
    try {
        // List all objects inside the extractor folder
        const s3FolderKey = `extractors/${extId}/`;
        const params = {
            Bucket: S3_ROOT_BUCKET,
            Prefix: s3FolderKey,
        };
        
        const data = await s3.listObjectsV2(params).promise();
        if (data.Contents.length > 0) {
            const deleteParams = {
                Bucket: S3_ROOT_BUCKET,
                Delete: {
                    Objects: data.Contents.map((obj) => ({ Key: obj.Key })),
                    Quiet: false
                }
            };
            await s3.deleteObjects(deleteParams).promise();
            console.log("Object in S3 Deleted Successfully.");
        }

        // Delete the extractor from DynamoDB
        const deleteParams = {
            TableName: tableName,
            Key: {
                extId: extId,
            },
        };
        await docClient.delete(deleteParams).promise();

        return { statusCode: 200, body: `Extractor with ID ${extId} and extractor deleted` };
    } catch (error) {
        console.error('Error deleting extractor:', error.message || error);
        return { statusCode: 500, error: 'deleteExtractor: Internal Server Error' };
    }
};


export default { 
    writeExtractor: writeExtractor,
    readAllExtractors: readAllExtractors,
    readExtractor: readExtractor,
    extractorExists: extractorExists,
    createDocument: createDocument,
    readDocument: readDocument,
    deleteDocument: deleteDocument,
    deleteExtractor: deleteExtractor,
    
    
};