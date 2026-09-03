@binary
Feature: Binary file processing

  A binary object is emitted as one event with its payload Base64 encoded.

  Scenario: Binary file produces a Base64 envelope
    Given I have a binary file
    When I upload and process the file
    Then I receive exactly 1 events within 30 seconds
    And all events are valid F2E envelopes
    And all event record numbers from 1 to 1 are present
