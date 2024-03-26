
# Operators

This document investigates how some operators can be derived from other operators, in order to identify ways to simplify the operator library by phasing out redundant operators and/or variants, as well as potential gaps in the standard library.

## 0th-order

The `then` operator (provided by this plugin) is a sort of "0th-order" operator because it can be used to implement any operator (except for those with multiple inputs).

## 1st-order

The `then.nf` example pipeline shows how to implement the following operators with `then`.

basic:
- `ifEmpty`
- `set`, `tap` (variable assignment)
- `subscribe` (iteration w/ no inter-dependency, only side effects)

filtering:
- `distinct`
- `filter` (if statement)
- `first`
- `last`
- `take`
- `until`

transforming:
- `buffer`
- `collate`
- `flatMap`
- `flatten`
- `groupTuple`
- `map` (expression)
- `reduce` (iteration w/ inter-dependency)
- `transpose`

combining:
- `combine`
- `concat`
- `cross`
- `join`
- `merge`
- `mix`

## 2nd-order

- `branch`
  ```groovy
  ch | branch { /* ... */ } == [
    label1: ch | filter(cond1),
    label2: ch | filter(cond2),
    // ...
  ]
  ```

- `collect`
  ```groovy
  ch | collect == ch | reduce([]) { acc, v -> acc << v ; acc }
  ```

- `collectFile`
  ```groovy
  ch | collectFile('file.txt') == ch | reduce(file('file.txt')) { acc, v -> acc << v.text ; acc }
  ```

- `count`
  ```groovy
  ch | count == ch | reduce(0) { acc, v -> acc + 1 }
  ```

- `countFasta`
- `countFastq`
- `countJson`
- `countLines`
  ```groovy
  ch | countLines == ch | flatMap(splitText) | count
  ```

- `max`
  ```groovy
  ch | max == ch | reduce(Integer.MIN_VALUE) { acc, v -> Math.max(acc, v) }
  ```

- `min`
  ```groovy
  ch | min == ch | reduce(Integer.MAX_VALUE) { acc, v -> Math.min(acc, v) }
  ```

- `multiMap`
  ```groovy
  ch | multiMap { /* ... */ } == [
    label1: ch | map(mapper1),
    label2: ch | map(mapper2),
    // ...
  ]
  ```

- `randomSample`
  ```groovy
  ch | randomSample(k) == ch | collect | flatMap(randomSample)
  ```

- `splitCsv`
- `splitFasta`
- `splitFastq`
- `splitJson`
- `splitText`
  ```groovy
  ch | splitCsv == ch | flatMap(splitCsv)
  ```

- `sum`
  ```groovy
  ch | count == ch | reduce(0) { acc, v -> acc + v }
  ```

- `toInteger`
- `toLong`
- `toFloat`
- `toDouble`
  ```groovy
  ch | toInteger == ch | map { v -> v as Integer }
  ```

- `toList`
- `toSortedList`
  ```groovy
  ch | toList == ch | collect(flat: false)
  ```

- `unique`
  ```groovy
  ch | unique == ch | collect | flatMap { v -> v.unique() }
  ```

- `view`
- `dump`
  ```groovy
  ch | view == ch | subscribe { val ->
    println(val)
  }
  ```

## Notes

Operators that could be phased out:

- `collectFile`: replace with stdlib function `mergeCsv`, `mergeText`
  - use `groupTuple` for grouping
  - use `List::sort` for sorting

- `countFasta`, `countFastq`, `countJson`, `countLines`: use stdlib functions instead

- `merge`: remove when there is better support for record types

- `randomSample`: replace with stdlib function

- `splitCsv`, `splitFasta`, `splitFastq`, `splitJson`, `splitText`: use stdlib functions instead

- `toInteger`, `toLong`, `toFloat`, `toDouble`: use `map` instead

- `collect`, `toList`, `toSortedList`: collapse to `collect`, which should:
  - collect source values into a list
  - not flatten items, do `flatMap | collect` instead
  - not sort items, do `collect | map(List::sort)` instead

- `collate`: remove first variant in favor of `buffer`, rename second variant to `window`

- `combine`, `cross`: collapse to `cross`, which should:
  - accept channel or collection inputs
  - select pairs with matching key only if `by` option is specified (can be integer or closure)
  - not flatten output, use `flatMap` instead

Operators that could be simplified:

- `collect`: mapping closure, `flat` and `sort` options
- `count`: filtering closure
- `first`: filtering closure
- `flatMap`: map flattening
- `ifEmpty`: closure
- `unique`: mapping closure

Missing operators:

- `scan`
