# Exberry DAML Integration

This is a DAML integration implementing the [Exberry APIs](https://docs.exberry.io/). The integration is deployed configured and launched in [project:DABL](https://www.projectdabl.com). It will run as a DAML Party on your ledger that will send and receive order management messages as well as order book updates and executions. Inbound and Outbound order messages are translated to DAML contracts that can be incorporated to a custom DAML workflow.

## To configure

The integration requires a set of credentials and URLs for connecting and authenticating to Exberry. 


- Username: your user name for the Management API
- Password: your password for the Management API
- Client Id: your client’s id for the Management API
- Trading API URL: the secure web socket url to Exberry’s matching engine
- Admin API URL: the Management API url
- Token URL: the url to request a Management API token
- API Key: The API key for authenticating to Exberry’s matching engine
- Secret: The API secret for authenticating to Exberry’s matching engine

> This integration is under active development. Please raise any Github issues or direct your questions to the [DAML forum](https://discuss.daml.com/)
