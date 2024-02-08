import ext from './api/extractors.mjs';

export const handler = async (event) => {
  // TODO implement
  console.info(event);

  let routingFns = {
    "GET /api/extractors"                                        : ext.readAllExtractors,
    "PUT /api/extractors"                                        : ext.createExtractor,
    "PUT /api/extractors/{extId}/ping"                           : ext.pingExtractor,
    "GET /api/extractors/{extId}"                                : ext.readExtractor,
    "DELETE /api/extractors/{extId}"                             : ext.deleteExtractor,
    "PUT /api/extractors/{extId}/{docType}/documents"            : ext.createDocument,
    "GET /api/extractors/{extId}/{docType}/documents/{docId}"    : ext.readDocument,
    "DELETE /api/extractors/{extId}/{docType}/documents/{docId}" : ext.deleteDocument,
    "PUT /api/extractors/purge"                                  : ext.purgeExtractor,
  };
  
  let fn = routingFns[event["routeKey"]];
  console.log("Route Key: ", event.routeKey);
  let data = fn ? await fn(event) : { statusCode: 501, error: "API not implemented!!!" };
  
  const response = {
    body: (data.body || data.error),
  };
  return response;
};
//Uploading via VSCode => Lambda Function 
