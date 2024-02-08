import ext from './ctrl-extractors.mjs';
import parser from 'lambda-multipart-parser';

// Extract extId from path parameters
function getExtId(event) {
    let extId = (event.pathParameters || {}).extId;
    console.log("Extractor ID: ", extId);
    return extId;
}

//Extract docType from path parameters
function getDocType(event) {
    let doctype = (event.pathParameters || {}).docType;
    console.log("Document Type: ", doctype);
    return doctype;
}

//Extract docId from path parameters
function getDocId(event) {
    let docId = (event.pathParameters || {}).docId;
    console.log("Document ID: ", docId);
    return docId;
}



const pingExtractor          = async (event) => ext.pingExtractor(getExtId(event));
const createExtractor        = async (event) => ext.createExtractor(JSON.parse(event.body));
const readAllExtractors      = async (event) => ext.readAllExtractors();
const readExtractor          = async (event) => ext.readExtractor(getExtId(event));
const deleteExtractor        = async (event) => ext.deleteExtractor(getExtId(event));
const purgeExtractor         = async (event) => ext.purgeExtractor();
const readDocument           = async (event) => ext.readDocument(getExtId(event), getDocType(event), getDocId(event));
const deleteDocument         = async (event) => ext.deleteDocument(getExtId(event), getDocType(event), getDocId(event));
const createDocument         = async (event) => {
    const resultDocs = await parser.parse(event);

    try {
        if (!resultDocs.files || resultDocs.files.length === 0) {
            return { statusCode: 400, body: "No files uploaded" };
        }

        const documents = resultDocs.files.map(file => ({
            originalName: file.filename,
            buffer: Buffer.from(file.content, 'base64')
        }));

       console.log("Documents: ", documents);
       let result = await ext.createDocument(getExtId(event), getDocType(event), documents);
       
       return { statusCode: 201, body: result };
    } catch (error) {
        console.error('Error parsing event body:', error);
        return { statusCode: 500, error: 'Invalid request body' };
    }
};


export default { 
    createExtractor  : createExtractor,
    readAllExtractors: readAllExtractors,
    pingExtractor    : pingExtractor,
    readExtractor    : readExtractor,
    createDocument   : createDocument,
    readDocument     : readDocument,
    deleteDocument   : deleteDocument,
    deleteExtractor  : deleteExtractor,
    purgeExtractor   : purgeExtractor,
    
    
    
    
};
