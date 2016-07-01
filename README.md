## Apache NiFi processor for creating PDF/DOC/EXCEL documents.

Creates PDF/DOC/EXCEL report using flowfile XML payload as data input. A new FlowFile is created with report document content and is routed to the 'success' relationship. If the report generation fails, the original FlowFile is routed to the 'failure' relationship.

BIRT is used to generate documents.
