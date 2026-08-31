Feature: CSV file processing

  CSV files are processed as plain data rows — no header skipping.
  Each row produces one event with data.fields populated.
  Files with ≤1000 rows use single-chunk mode (MaxRecordLengthBytes=0);
  larger files set MaxRecordLengthBytes to enable variable chunking.

  Scenario: Empty CSV file is rejected
    Given I have an empty "csv" file
    When I upload and process the file
    Then no events are produced within 15 seconds

  Scenario Outline: CSV file with <count> records — full validation
    Given I have a CSV file with <count> records
    When I upload and process the file
    Then I receive exactly <count> events within <timeout> seconds
    And all events are valid F2E envelopes
    And all event record numbers from 1 to <count> are present

    Examples:
      | count | timeout |
      | 1     | 30      |
      | 2     | 30      |
      | 1000  | 90      |

  Scenario Outline: CSV file with <count> records — count validation only
    Given I have a CSV file with <count> records
    When I upload and process the file
    Then I receive exactly <count> events within <timeout> seconds

    Examples:
      | count | timeout |
      | 10000 | 180     |

  @load
  Scenario: CSV file with 1000000 records
    Given I have a CSV file with 1000000 records
    When I upload and process the file
    Then I receive exactly 1000000 events within 600 seconds
