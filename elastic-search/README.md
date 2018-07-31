# elastic-search-webhook module

This module is the interface between the Webhook Elastic Search and the data sync process.

### Usage

```javascript
var ElasticSearchSync = require('elastic-search')
var elastic = ElasticSearchSync()
```

Elastic Search configuration is done through the following envrionment variables.

```
ELASTIC_SEARCH_SERVER
ELASTIC_SEARCH_USER
ELASTIC_SEARCH_PASSWORD
```

WebHook `stream-interface`, the default package interface, depends on `ELASTIC_SEARCH_INDEX` being defined.
