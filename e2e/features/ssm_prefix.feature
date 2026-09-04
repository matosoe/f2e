@ssm
Feature: S3 prefix configuration from SSM

  The Organizer must resolve the input type and limits from the SSM document
  associated with the bucket and S3 prefix, without an explicit request.

  Scenario: Text file uploaded to the configured prefix
    Given I have a text file with 3 records
    When I upload the file through its configured S3 prefix
    Then I receive exactly 3 events within 30 seconds
    And all events are valid F2E envelopes
    And all event record numbers from 1 to 3 are present
