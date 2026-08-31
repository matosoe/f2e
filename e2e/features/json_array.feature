Feature: JSON array file processing

  The file contains a single top-level JSON array where each element becomes one event
  with data.raw set to the element JSON.  MaxBytesPerElement=256 is always required by
  the organizer for planning; it also controls the chunk size for large files.

  Scenario: Empty JSON array file produces no events
    Given I have a JSON array file with 0 elements
    When I upload and process the file
    Then no events are produced within 15 seconds

  Scenario Outline: JSON array file with <count> elements — full validation
    Given I have a JSON array file with <count> elements
    When I upload and process the file
    Then I receive exactly <count> events within <timeout> seconds
    And all events are valid F2E envelopes
    And all event record numbers from 1 to <count> are present

    Examples:
      | count | timeout |
      | 1     | 30      |
      | 2     | 30      |
      | 1000  | 90      |

  Scenario Outline: JSON array file with <count> elements — count validation only
    Given I have a JSON array file with <count> elements
    When I upload and process the file
    Then I receive exactly <count> events within <timeout> seconds

    Examples:
      | count | timeout |
      | 10000 | 180     |

  @load
  Scenario: JSON array file with 1000000 elements
    Given I have a JSON array file with 1000000 elements
    When I upload and process the file
    Then I receive exactly 1000000 events within 600 seconds
