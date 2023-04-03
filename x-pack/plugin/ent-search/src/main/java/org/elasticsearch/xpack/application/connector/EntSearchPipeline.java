/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

/**
 * The default Enterprise Search pipeline used by connectors and crawlers
 */
public class EntSearchPipeline {
    @SuppressWarnings("checkstyle:LineLength")
    private static final String pipelineDefinition =
        """
                    {
                      "version": 1,
                      "description": "Generic Enterprise Search ingest pipeline",
                      "_meta": {
                        "managed_by": "Enterprise Search",
                        "managed": true
                      },
                      "processors": [
                        {
                          "attachment": {
                            "description": "Extract text from binary attachments",
                            "field": "_attachment",
                            "target_field": "_extracted_attachment",
                            "ignore_missing": true,
                            "indexed_chars_field": "_attachment_indexed_chars",
                            "if": "ctx?._extract_binary_content == true",
                            "on_failure": [
                              {
                                "append": {
                                  "description": "Record error information",
                                  "field": "_ingestion_errors",
                                  "value": "Processor 'attachment' in pipeline '{{ _ingest.on_failure_pipeline }}' failed with message '{{ _ingest.on_failure_message }}'"
                                }
                              }
                            ]
                          }
                        },
                        {
                          "set": {
                            "tag": "set_body",
                            "description": "Set any extracted text on the 'body' field",
                            "field": "body",
                            "copy_from": "_extracted_attachment.content",
                            "ignore_empty_value": true,
                            "if": "ctx?._extract_binary_content == true",
                            "on_failure": [
                              {
                                "append": {
                                  "description": "Record error information",
                                  "field": "_ingestion_errors",
                                  "value": "Processor 'set' with tag 'set_body' in pipeline '{{ _ingest.on_failure_pipeline }}' failed with message '{{ _ingest.on_failure_message }}'"
                                }
                              }
                            ]
                          }
                        },
                        {
                          "gsub": {
                            "tag": "remove_replacement_chars",
                            "description": "Remove unicode 'replacement' characters",
                            "field": "body",
                            "pattern": "�",
                            "replacement": "",
                            "ignore_missing": true,
                            "if": "ctx?._extract_binary_content == true",
                            "on_failure": [
                              {
                                "append": {
                                  "description": "Record error information",
                                  "field": "_ingestion_errors",
                                  "value": "Processor 'gsub' with tag 'remove_replacement_chars' in pipeline '{{ _ingest.on_failure_pipeline }}' failed with message '{{ _ingest.on_failure_message }}'"
                                }
                              }
                            ]
                          }
                        },
                        {
                          "gsub": {
                            "tag": "remove_extra_whitespace",
                            "description": "Squish whitespace",
                            "field": "body",
                            "pattern": "\\\\s+",
                            "replacement": " ",
                            "ignore_missing": true,
                            "if": "ctx?._reduce_whitespace == true",
                            "on_failure": [
                              {
                                "append": {
                                  "description": "Record error information",
                                  "field": "_ingestion_errors",
                                  "value": "Processor 'gsub' with tag 'remove_extra_whitespace' in pipeline '{{ _ingest.on_failure_pipeline }}' failed with message '{{ _ingest.on_failure_message }}'"
                                }
                              }
                            ]
                          }
                        },
                        {
                          "trim" : {
                            "description": "Trim leading and trailing whitespace",
                            "field": "body",
                            "ignore_missing": true,
                            "if": "ctx?._reduce_whitespace == true",
                            "on_failure": [
                              {
                                "append": {
                                  "description": "Record error information",
                                  "field": "_ingestion_errors",
                                  "value": "Processor 'trim' in pipeline '{{ _ingest.on_failure_pipeline }}' failed with message '{{ _ingest.on_failure_message }}'"
                                }
                              }
                            ]
                          }
                        },
                        {
                          "remove": {
                            "tag": "remove_meta_fields",
                            "description": "Remove meta fields",
                            "field": [
                              "_attachment",
                              "_attachment_indexed_chars",
                              "_extracted_attachment",
                              "_extract_binary_content",
                              "_reduce_whitespace",
                              "_run_ml_inference"
                            ],
                            "ignore_missing": true,
                            "on_failure": [
                              {
                                "append": {
                                  "description": "Record error information",
                                  "field": "_ingestion_errors",
                                  "value": "Processor 'remove' with tag 'remove_meta_fields' in pipeline '{{ _ingest.on_failure_pipeline }}' failed with message '{{ _ingest.on_failure_message }}'"
                                }
                              }
                            ]
                          }
                        }
                      ]
                    }
            """;

    public static String getPipelineDefinition() {
        return pipelineDefinition;
    }
}
