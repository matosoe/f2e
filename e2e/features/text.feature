Feature: Variable-length text file processing

  Each line becomes one event with data.raw set to the line content.
  Files with ≤1000 lines use single-chunk mode (MaxRecordLengthBytes=0);
  larger files set MaxRecordLengthBytes=64 to enable parallel chunk processing.

  @smoke @regression
  Scenario: Empty text file is rejected
    Given I have an empty "text" file
    When I upload and process the file
    Then no events are produced within 15 seconds

  @smoke @regression
  Scenario Outline: Text file with <count> records — full validation
    Given I have a text file with <count> records
    When I upload and process the file
    Then I receive exactly <count> events within <timeout> seconds
    And all events are valid F2E envelopes
    And all event record numbers from 1 to <count> are present

    Examples:
      | count | timeout |
      | 1     | 30      |
      | 2     | 30      |
      | 1000  | 90      |

  @regression
  Scenario Outline: Text file with <count> records — count validation only
    Given I have a text file with <count> records
    When I upload and process the file
    Then I receive exactly <count> events within <timeout> seconds

    Examples:
      | count | timeout |
      | 10000 | 180     |

  @load
  Scenario: Text file with 1000000 records
    Given I have a text file with 1000000 records
    When I upload and process the file
    Then I receive exactly 1000000 events within 600 seconds
