Feature: Fixed-width file processing

  Fixed-width records have exactly 100 bytes each (99 printable ASCII + LF).
  The organizer splits files into chunks of F2E_RECORDS_PER_CHUNK records (default 1000),
  so files with ≤1000 records produce a single chunk and are fully validated;
  larger files produce multiple chunks and are validated by count only.

  Scenario: Empty fixed-width file is rejected
    Given I have an empty "fixed-width" file
    When I upload and process the file
    Then no events are produced within 15 seconds

  Scenario Outline: Fixed-width file with <count> records — full validation
    Given I have a fixed-width file with <count> records
    When I upload and process the file
    Then I receive exactly <count> events within <timeout> seconds
    And all events are valid F2E envelopes
    And all event record numbers from 1 to <count> are present

    Examples:
      | count | timeout |
      | 1     | 30      |
      | 2     | 30      |
      | 1000  | 90      |

  Scenario Outline: Fixed-width file with <count> records — count validation only
    Given I have a fixed-width file with <count> records
    When I upload and process the file
    Then I receive exactly <count> events within <timeout> seconds

    Examples:
      | count | timeout |
      | 10000 | 180     |

  @load
  Scenario: Fixed-width file with 1000000 records
    Given I have a fixed-width file with 1000000 records
    When I upload and process the file
    Then I receive exactly 1000000 events within 600 seconds
