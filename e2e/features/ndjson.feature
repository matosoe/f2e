Feature: NDJSON (Newline-Delimited JSON) file processing

  NDJSON is treated identically to JSONL — each line must be a valid JSON object.
  Files with ≤1000 lines use single-chunk mode; larger files enable variable chunking.

  Scenario: Empty NDJSON file is rejected
    Given I have an empty "ndjson" file
    When I upload and process the file
    Then no events are produced within 15 seconds

  Scenario Outline: NDJSON file with <count> records — full validation
    Given I have an NDJSON file with <count> records
    When I upload and process the file
    Then I receive exactly <count> events within <timeout> seconds
    And all events are valid F2E envelopes
    And all event record numbers from 1 to <count> are present

    Examples:
      | count | timeout |
      | 1     | 30      |
      | 2     | 30      |
      | 1000  | 90      |

  Scenario Outline: NDJSON file with <count> records — count validation only
    Given I have an NDJSON file with <count> records
    When I upload and process the file
    Then I receive exactly <count> events within <timeout> seconds

    Examples:
      | count | timeout |
      | 10000 | 180     |

  @load
  Scenario: NDJSON file with 1000000 records
    Given I have an NDJSON file with 1000000 records
    When I upload and process the file
    Then I receive exactly 1000000 events within 600 seconds
