Feature: Multi-line file processing with header and trailer

  The generated file has:
    H<date>          — one header line (position 0 marker "H"), skipped by the reader
    D<seq><data>...  — N data lines (break marker "D" at position 0), each becomes one event
    T<count>         — one trailer line (marker "T"), skipped by the reader

  The organizer uses BreakMarker="D", AcceptedPrefixes=["D"], MaxBytesPerRecord=128.
  Files with ≤1000 data records fit in a single chunk; larger files produce multiple chunks.

  @smoke @regression
  Scenario: Empty multi-line file is rejected
    Given I have an empty "multi-line" file
    When I upload and process the file
    Then no events are produced within 15 seconds

  @smoke @regression
  Scenario Outline: Multi-line file with header, <count> data records, and trailer — full validation
    Given I have a multi-line file with header, <count> data records, and trailer
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
  Scenario Outline: Multi-line file with header, <count> data records, and trailer — count validation only
    Given I have a multi-line file with header, <count> data records, and trailer
    When I upload and process the file
    Then I receive exactly <count> events within <timeout> seconds

    Examples:
      | count | timeout |
      | 10000 | 180     |

  @load
  Scenario: Multi-line file with header, 1000000 data records, and trailer
    Given I have a multi-line file with header, 1000000 data records, and trailer
    When I upload and process the file
    Then I receive exactly 1000000 events within 600 seconds
