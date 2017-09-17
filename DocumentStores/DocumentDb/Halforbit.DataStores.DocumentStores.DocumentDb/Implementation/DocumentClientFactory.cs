using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Halforbit.DataStores.DocumentStores.DocumentDb.Implementation
{
    static class DocumentClientFactory
    {
        static IDictionary<Uri, DocumentClient> _documentClients = 
            new ConcurrentDictionary<Uri, DocumentClient>();

        public static DocumentClient GetDocumentClient(
            Uri endpoint, 
            string authKey)
        {
            var documentClient = default(DocumentClient);

            if(_documentClients.TryGetValue(endpoint, out documentClient))
            {
                return documentClient;
            }

            documentClient = new DocumentClient(
                endpoint, 
                authKey, 
                new ConnectionPolicy { EnableEndpointDiscovery = false },
                ConsistencyLevel.Session);

            _documentClients.Add(endpoint, documentClient);

            return documentClient;
        }
    }
}
