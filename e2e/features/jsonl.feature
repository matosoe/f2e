Feature: JSON Lines (JSONL) file processing

  Each line is a valid JSON object that becomes one event with data.raw set to the line.
  Files with ≤1000 lines use single-chunk mode; larger files enable variable chunking via
  MaxRecordLengthBytes=128 so multiple workers process the file in parallel.

  Scenario: Empty JSONL file is rejected
    Given I have an empty "jsonl" file
    When I upload and process the file
    Then no events are produced within 15 seconds

  Scenario Outline: JSONL file with <count> records — full validation
    Given I have a JSONL file with <count> records
    When I upload and process the file
    Then I receive exactly <count> events within <timeout> seconds
    And all events are valid F2E envelopes
    And all event record numbers from 1 to <count> are present

    Examples:
      | count | timeout |
      | 1     | 30      |
      | 2     | 30      |
      | 1000  | 90      |

  Scenario Outline: JSONL file with <count> records — count validation only
    Given I have a JSONL file with <count> records
    When I upload and process the file
    Then I receive exactly <count> events within <timeout> seconds

    Examples:
      | count | timeout |
      | 10000 | 180     |

  @load
  Scenario: JSONL file with 1000000 records
    Given I have a JSONL file with 1000000 records
    When I upload and process the file
    Then I receive exactly 1000000 events within 600 seconds
