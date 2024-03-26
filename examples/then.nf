
include { then ; thenMany } from 'plugin/nf-boost'

@ValueObject
class BranchCriteria {
  String name
  Closure predicate
}

def boostBranch(ch, List<BranchCriteria> criteria) {
  final names = criteria.collect( c -> c.name )
  ch.thenMany(emits: names) { it ->
    for( def c : criteria )
      if( c.predicate(it) )
        emit(c.name, it)
  }
}

def boostBuffer(ch, int size, boolean remainder = false) {
  if( size <= 0 )
    error 'buffer `size` parameter must be greater than zero'

  def buffer = []
  ch.then(
    onNext: { it ->
      buffer << it
      if( buffer.size() == size ) {
        emit(buffer)
        buffer = []
      }
    },
    onComplete: {
      if( remainder && buffer.size() > 0 )
        emit(buffer)
    }
  )
}

def boostCollect(ch) {
  final result = []
  ch.then(
    singleton: true,
    onNext: { it ->
      result << it
    },
    onComplete: {
      if( result )
        emit(result)
    }
  )
}

def boostDistinct(ch) {
  def first = true
  def prev
  ch.then { it ->
    if( first ) {
      first = false
      emit(it)
    }
    else if( it != prev )
      emit(it)
    prev = it
  }
}

def boostFilter(ch, Closure predicate) {
  ch.then { it ->
    if( predicate(it) )
      emit(it)
  }
}

def boostFirst(ch) {
  def first = true
  ch.then(singleton: true) { it ->
    if( !first )
      return
    emit(it)
    first = false
    done()
  }
}

def boostFlatMap(ch, Closure mapper) {
  ch.then(singleton: false) { it ->
    final result = mapper != null ? mapper(it) : it
    if( result instanceof Collection )
      result.each( v -> emit(v) )
    else
      emit(result)
  }
}

def boostGroupTuple(Map opts = [:], ch) {
  final size = opts.size ?: 0
  final remainder = opts.remainder ?: false

  if( size < 0 )
    error 'groupTuple `size` parameter must be non-negative'

  final indices = opts.by == null
    ? [ 0 ]
    : opts.by instanceof Integer
      ? [ opts.by ]
      : null

  if( indices == null || indices !instanceof List )
    error "groupTuple `by` parameter must be an integer or list of integers: '${opts.by}'"

  final groups = [:]
  ch.then(
    singleton: false,
    onNext: { val ->
      if( val !instanceof List )
        error "In `groupTuple` operatoer -- expected a tuple but received: ${val} [${val.class.simpleName}]"

      final tuple = (List)val
      final key = tuple[indices]
      final len = tuple.size()
      final values = groups.getOrCreate(key) {
        (0..<len).collect { i ->
          i in indices ? tuple[i] : []
        }
      }

      int count = -1
      for( int i = 0; i < len; i++ ) {
        if( i in indices )
          continue
        if( values[i] == null )
          values[i] = []
        final list = values[i]
        list << tuple[i]
        count = list.size()
      }

      if( size != 0 && size == count ) {
        emit( values as ArrayList )
        groups.remove(key)
      }
    },
    onComplete: {
      groups.each { key, values ->
        if( !remainder && size != 0 ) {
          final list = values.find( v -> v instanceof List )
          if( list.size() != size )
            return
        }

        emit( values as ArrayList )
      }
    }
  )
}

def boostIfEmpty(ch, value) {
  def empty = true
  ch.then(
    onNext: { it ->
      emit(it)
      empty = false
    },
    onComplete: {
      if( empty )
        emit(value)
    }
  )
}

def boostLast(ch) {
  def last
  ch.then(
    singleton: true,
    onNext: { it ->
      last = it
    },
    onComplete: { emit(last) }
  )
}

def boostMap(ch, Closure mapper) {
  ch.then { it ->
    emit(mapper(it))
  }
}

@ValueObject
class MultiMapCriteria {
  String name
  Closure transform
}

def boostMultiMap(ch, List<MultiMapCriteria> criteria) {
  final names = criteria.collect( c -> c.name )
  ch.thenMany(emits: names) { it ->
    for( def c : criteria )
      emit(c.name, c.transform(it))
  }
}

def boostReduce(ch, seed, Closure accumulator) {
  def result = seed
  ch.then(
    singleton: true,
    onNext: { it ->
      result = accumulator(result, it)
    },
    onComplete: { emit(result) }
  )
}

def boostScan(ch, seed, Closure accumulator) {
  def result = seed
  ch.then { it ->
    result = accumulator(result, it)
    emit(result)
  }
}

def boostTake(ch, int n) {
  def count = 0
  ch.then(singleton: false) { it ->
    if( n != 0 )
      emit(it)
    if( n >= 0 && ++count >= n )
      done()
  }
}

def boostTranspose(ch, by = null, boolean remainder = false) {
  final cols = by == null
    ? []
    : by instanceof List ? by : [by]

  ch.then(singleton: false) { val ->
    if( val !instanceof List )
      error "In `transpose` operatoer -- expected a tuple but received: ${val} [${val.class.simpleName}]"

    final tuple = (List)val
    cols.eachWithIndex { col, i ->
      final el = tuple[col]
      if( el !instanceof List )
        error "In `transpose` operator -- expected a list at tuple index ${col} but received: ${el} [${el.class.simpleName}]"
    }

    final indices = cols ?: {
      final result = []
      tuple.eachWithIndex { el, i ->
        if( el instanceof List )
          result << i
      }
      result
    }.call()

    final max = indices.collect(i -> tuple[i].size()).max()

    for( int i : 0..<max ) {
      final result = []
      for( int k : 0..<tuple.size() ) {
        if( k in indices ) {
          final list = tuple[k]
          if( i < list.size() )
            result[k] = list[i]
          else if( remainder )
            result[k] = null
          else
            return
        }
        else
          result[k] = tuple[k]
      }
      emit(result)
    }
  }
}

def boostUntil(ch, Closure predicate) {
  ch.then { it ->
    if( predicate(it) )
      done()
    else
      emit(it)
  }
}

def boostWindow(ch, int size, int step, boolean remainder = true) {
  if( size <= 0 )
    error 'window `size` parameter must be greater than zero'
  if( step <= 0 )
    error 'window `step` parameter must be greater than zero'

  def windows = []
  def index = 0
  ch.then(
    singleton: false,
    onNext: { it ->
      index += 1
      if( index % step == 0 )
        windows << []

      windows.each { window -> window << it }

      final window = windows.head()
      if( window.size() == size ) {
        emit(window)
        windows = windows.tail()
      }
    },
    onComplete: {
      if( remainder && windows.size() > 0 )
        windows.each { emit(it) }
    }
  )
}

def parseQueueValues(String queue) {
  if( queue.contains('..') ) {
    final (min, max) = queue.tokenize('..')
    return (min as int) .. (max as int)
  }
  else {
    return queue.tokenize(',')
  }
}

def asInteger( value ) {
  if( value instanceof Integer )
    return value
  if( value instanceof String )
    return value.size()
  error "cannot coerce value to integer: ${value} [${value.class.simpleName}]"
}

params.empty = false
params.value = null
params.queue = '1..10'

workflow {
  ch = params.empty
    ? Channel.empty()
    : params.value
      ? Channel.value( params.value )
      : Channel.fromList( parseQueueValues(params.queue) )

  // branch
  ch_branch = boostBranch(ch, [
      new BranchCriteria('div1', { it % 1 == 0 }),
      new BranchCriteria('div2', { it % 2 == 0 }),
      new BranchCriteria('div3', { it % 3 == 0 }),
    ])

  Channel.empty()
    .mix(
      ch_branch.div1.map { "div1: ${it}" },
      ch_branch.div2.map { "div2: ${it}" },
      ch_branch.div3.map { "div3: ${it}" }
    )
    .dump(tag: 'branch')

  ch_branch.div1.dump(tag: 'branch:div1')
  ch_branch.div2.dump(tag: 'branch:div2')
  ch_branch.div3.dump(tag: 'branch:div3')

  // buffer
  boostBuffer(ch, 3, true)
    .dump(tag: 'buffer')

  // collect
  boostCollect(ch)
    .dump(tag: 'collect')

  // distinct
  ch_rev = ch | collect | flatMap { it.reverse() }
  boostDistinct(ch.concat(ch_rev))
    .dump(tag: 'distinct')

  // filter
  boostFilter(ch) { it > 5 }
    .dump(tag: 'filter')

  // first
  boostFirst(ch)
    .dump(tag: 'first')

  // flatMap
  boostFlatMap(ch) { v -> [v] * asInteger(v) }
    .dump(tag: 'flatMap')

  // groupTuple
  ch_transposed = ch | map { v -> [v, [v] * asInteger(v)] } | transpose
  boostGroupTuple( ch_transposed.dump(tag: 'groupTuple'), remainder: true )
    .dump(tag: 'groupTuple')

  // ifEmpty
  boostIfEmpty(ch, 'foo')
    .dump(tag: 'ifEmpty')

  // last
  boostLast(ch)
    .dump(tag: 'last')

  // map
  boostMap(ch) { it * 2 }
    .dump(tag: 'map')

  // multiMap
  ch_multi = boostMultiMap(ch, [
      new MultiMapCriteria('mul1', { it * 1 }),
      new MultiMapCriteria('mul2', { it * 2 }),
      new MultiMapCriteria('mul3', { it * 3 }),
    ])

  Channel.empty()
    .mix(
      ch_multi.mul1.map { "mul1: ${it}" },
      ch_multi.mul2.map { "mul2: ${it}" },
      ch_multi.mul3.map { "mul3: ${it}" }
    )
    .dump(tag: 'multiMap')

  ch_multi.mul1.dump(tag: 'multiMap:mul1')
  ch_multi.mul2.dump(tag: 'multiMap:mul2')
  ch_multi.mul3.dump(tag: 'multiMap:mul3')

  // reduce
  boostReduce(ch, 0) { acc, v -> acc + v }
    .dump(tag: 'reduce')

  // scan
  boostScan(ch, 0) { acc, v -> acc + v }
    .dump(tag: 'scan')

  // take
  boostTake(ch, 3)
    .dump(tag: 'take')

  // transpose
  ch_grouped = ch.map( v -> [v, 1..v as ArrayList] )
  boostTranspose( ch_grouped.dump(tag: 'transpose') )
    .dump(tag: 'transpose')

  // until
  boostUntil(ch) { v -> v == 7 }
    .dump(tag: 'until')

  // window
  boostWindow(ch, 3, 1, true)
    .dump(tag: 'window')
}
