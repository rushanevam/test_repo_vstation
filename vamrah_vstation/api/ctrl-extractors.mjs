/*  Handles business logic for managing the extractor resources.  
The business logic written independent of the storage. */

import vief from './vief-utils.mjs';
import ext from './ddb-extractors.mjs';

const KEEP_ALIVE_TIME = 31536000000;


//Ping an Extractor with {extId}
const pingExtractor = async (extId) => {
    console.log("ctrl-extractor.mjs: Request Received: pingExtractor");
    if(!ext.extractorExists(extId))
        return { statusCode: 404, body: { error: "Extractor doesn't not exists" } };

    let extractor = ext.readExtractor(extId);
    if(new Date(extractor.expiry) < new Date())
        return { statusCode: 404, body: { error: "Extractor timed out" } };

    extractor.expiry = new Date(new Date().getTime() + KEEP_ALIVE_TIME).toISOString();
    ext.writeExtractor(extractor);

    return { statusCode: 200, body: `Extractor pinged successfully` };
};

// Creates a new extractor by adding a new entry in the extractor table and creating an empty container to hold documents
const createExtractor = async (extractor) => {
    
    // Validate required properties in the extractor object
    if (!extractor || !extractor.name || !extractor.description) {
        throw new Error('Invalid extractor data. Name and description are required.');
    }
    let random = Math.floor(Math.random() * 26);
    extractor["extId"] = vief.suuid(random);         // Create a new extractor Id, starting with character 'e'
    extractor["expiry"] = new Date(new Date().getTime() + KEEP_ALIVE_TIME).toISOString();
    extractor["docs"] = [];
    extractor["census"] = [];
    let result = await ext.writeExtractor(extractor);
    return { statusCode: 201, body: result };
};

// Read all Extractors from the extractor Table
const readAllExtractors = async() => {
    console.log("ctrl-extractor.mjs: Request Received");
    let result = await ext.readAllExtractors();
    return { statusCode: 200, body: result };
};

//Read an Extractor with {extId}
const readExtractor = async (extId) => {
    console.log("ctrl-extractor.mjs: Request Received: readExtractor");
    let result = await ext.readExtractor(extId);
    return { statusCode: 200, body: result };
};

//Write Extractor documents with Files
const createDocument = async (extId, docType, documents) => {
    console.log("ctrl-extractor.mjs: Request Received: createDocument");
    let result = await ext.createDocument(extId, docType, documents);
    return { statusCode: 201, body: result };
};

//Read a Document with docId
const readDocument = async(extId, docType, docId) => {
    console.log("ctrl-extractor.mjs: readDocument: Request Received: ", extId, docType, docId);
    let result = await ext.readDocument(extId, docType, docId);
    return result;
};

//Delete a Document with docId
const deleteDocument = async(extId, docType, docId) => {
    console.log("ctrl-extractor.mjs: deleteDocument: Request Received: ", extId, docType, docId);
    let result = await ext.deleteDocument(extId, docType, docId);
    return {statusCode: 201, body: result };
};

//Delete an Extractor with extId
const deleteExtractor = async(extId) => {
    console.log("ctrl-extractor.mjs: deleteExtractor: Request Received: ", extId);
    let result = await ext.deleteExtractor(extId);
    return {statusCode: 201, body: result };
};

//Purge Extractors
const purgeExtractor = async() => {
        console.log("ctrl-extractor.mjs: purgeExtractor: Request Received ");
        try {
        const extractors = (await ext.readAllExtractors()).body; //Read all Extractors
        //Filter Expired Extractors
        const expiredExtractors = extractors.filter(ext => new Date(ext.expiry) < new Date()); 
        // Delete expired extractors
        for (const ext of expiredExtractors) {
            await ext.deleteExtractor(ext.extId);
        }
        return { statusCode: 200, body: expiredExtractors };
    } catch (error) {
        console.error('Error purging extractors:', error.message || error);
        return { statusCode: 500, error: 'Internal Server Error' };
    }
};























export default { 
    createExtractor: createExtractor,
    readAllExtractors: readAllExtractors,
    readExtractor: readExtractor,
    pingExtractor: pingExtractor,
    createDocument: createDocument,
    readDocument: readDocument,
    deleteDocument: deleteDocument,
    deleteExtractor: deleteExtractor,
    purgeExtractor: purgeExtractor,
    
    
    
    
    
};