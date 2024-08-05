
include { then ; thenMany } from 'plugin/nf-boost'

@ValueObject
class BranchCriteria {
  String name
  Closure predicate
}

def boostBranch(ch, List<BranchCriteria> criteria) {
  def names = criteria.collect { c -> c.name }
  ch.thenMany(emits: names) { val ->
    criteria.each { c -> 
      if( c.predicate(val) )
        emit(c.name, val)
    }
  }
}

def boostBuffer(ch, int size, boolean remainder = false) {
  if( size <= 0 )
    error 'buffer `size` parameter must be greater than zero'

  def buffer = []
  ch.then(
    onNext: { val ->
      buffer << val
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
  def result = []
  ch.then(
    singleton: true,
    onNext: { val ->
      result << val
    },
    onComplete: {
      if( result )
        emit(result)
    }
  )
}

def boostConcat(ch, others) {
  def n = others.size() + 1
  def buffers = (1..n).collect { i -> [] }
  def completed = (1..n).collect { i -> false }
  def current = 0
  ch.then(
    *others,
    singleton: false,
    onNext: { val, i ->
      if( current == i )
        emit(val)
      else
        buffers[i] << val
    },
    onComplete: { i ->
      completed[i] = true
      while( current < n && completed[current] ) {
        current += 1
        buffers[current].each { val -> emit(val) }
      }
      if( current == n )
        done()
    }
  )
}

def boostCross(Map opts = [:], left, right) {
  if( opts.by != null && !(opts.by instanceof Integer) && !(opts.by instanceof List<Integer>) )
    error "cross `by` parameter must be an integer or list of integers: '${opts.by}'"

  def pivot = opts.by != null
    ? opts.by instanceof List ? opts.by : [ opts.by ]
    : null

  def leftValues = [:]
  def rightValues = [:]
  def count = 2
  left.then(
    right,
    onNext: { val, i ->
      def keys
      def values
      if( !pivot ) {
        keys = []
        values = [val]
      }
      else if( !(val instanceof List) ) {
        if( pivot != [0] )
          error "In `cross` operator -- expected a list but received: ${val} [${val.class.simpleName}]"
        keys = [val]
        values = []
      }
      else {
        keys = []
        values = []
        val.eachWithIndex { el, k -> 
          if( k in pivot )
            keys << el
          else
            values << el
        }
      }

      if( !(keys in leftValues) )
        leftValues[keys] = []
      if( !(keys in rightValues) )
        rightValues[keys] = []

      if( i == 0 ) {
        rightValues[keys].each { rval -> emit( [*keys, *values, *rval] ) }
        leftValues[keys] << values
      }
      else if( i == 1 ) {
        leftValues[keys].each { lval -> emit( [*keys, *lval, *values] ) }
        rightValues[keys] << values
      }
    },
    onComplete: { i ->
      count -= 1
      if( count == 0 )
        done()
    }
  )
}

def boostDistinct(ch) {
  def first = true
  def prev
  ch.then { val ->
    if( first ) {
      first = false
      emit(val)
    }
    else if( val != prev )
      emit(val)
    prev = val
  }
}

def boostFilter(ch, Closure predicate) {
  ch.then { val ->
    if( predicate(val) )
      emit(val)
  }
}

def boostFirst(ch) {
  def first = true
  ch.then(singleton: true) { val ->
    if( !first )
      return
    emit(val)
    first = false
    done()
  }
}

def boostFlatMap(ch, Closure mapper) {
  ch.then(singleton: false) { val ->
    def result = mapper != null ? mapper(val) : val
    if( result instanceof Collection )
      result.each { el -> emit(el) }
    else
      emit(result)
  }
}

def boostGroupTuple(Map opts = [:], ch) {
  def size = opts.size ?: 0
  def remainder = opts.remainder ?: false

  if( size < 0 )
    error 'groupTuple `size` parameter must be non-negative'

  def indices = opts.by == null
    ? [ 0 ]
    : opts.by instanceof Integer
      ? [ opts.by ]
      : null

  if( indices == null || !(indices instanceof List) )
    error "groupTuple `by` parameter must be an integer or list of integers: '${opts.by}'"

  def groups = [:]
  ch.then(
    singleton: false,
    onNext: { val ->
      if( !(val instanceof List) )
        error "In `groupTuple` operator -- expected a tuple but received: ${val} [${val.class.simpleName}]"

      def tuple = val as List
      def key = tuple[indices]
      def len = tuple.size()
      def values = groups.getOrCreate(key) {
        (0..<len).collect { i ->
          i in indices ? tuple[i] : []
        }
      }

      int count = -1
      len.times { i ->
        if( i in indices )
          return
        if( values[i] == null )
          values[i] = []
        def list = values[i]
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
          def list = values.find { v -> v instanceof List }
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
    onNext: { val ->
      emit(val)
      empty = false
    },
    onComplete: {
      if( empty )
        emit(value)
    }
  )
}

def boostJoin(Map opts = [:], left, right) {
  if( opts.by != null && !(opts.by instanceof Integer) && !(opts.by instanceof List<Integer>) )
    error "join `by` parameter must be an integer or list of integers: '${opts.by}'"

  def pivot = opts.by != null
    ? opts.by instanceof List ? opts.by : [ opts.by ]
    : [0]
  def failOnRemainder = opts.failOnRemainder ?: false

  def state = [:] // Map< keys , Map<index, values> >
  def count = 2
  left.then(
    right,
    onNext: { val, i ->
      if( !(val instanceof List) )
        error "In `join` operator -- expected a list but received: ${val} [${val.class.simpleName}]"
      def tuple = val as List
      def keys = []
      def values = []
      tuple.eachWithIndex { el, k ->
        if( k in pivot )
          keys << el
        else
          values << el
      }

      if( !state.containsKey(keys) )
        state[keys] = [:]

      def buffers = state[keys]
      if( buffers.containsKey(i) )
        error "In `join` operator -- ${i == 0 ? 'left' : 'right'} channel received multiple values with the same key: ${keys}"

      buffers[i] = values

      if( buffers.size() == 2 )
        emit( [*keys, *buffers[0], *buffers[1]] )
    },
    onComplete: { i ->
      count -= 1
      if( count == 0 ) {
        if( failOnRemainder ) {
          state.each { entry ->
            def keys = entry.key
            def buffers = entry.value
            if( buffers.size() == 1 )
              error "In `join` operator -- received value with unmatched key: ${keys}"
          }
        }
        done()
      }
    }
  )
}

def boostLast(ch) {
  def last
  ch.then(
    singleton: true,
    onNext: { val ->
      last = val
    },
    onComplete: { emit(last) }
  )
}

def boostMap(ch, Closure mapper) {
  ch.then { val ->
    emit(mapper(val))
  }
}

def boostMerge(ch, others) {
  // TODO: need to mark singleton channels so that they aren't consumed
  def count = others.size() + 1
  def buffers = (1..count).collect { i -> [] }
  ch.then(
    *others,
    onNext: { val, i ->
      buffers[i] << val
      if( buffers.every { buf -> buf.size() > 0 } )
        emit( buffers.collect { buf -> buf.pop() } )
    },
    onComplete: { i ->
      count -= 1
      if( count == 0 )
        done()
    }
  )
}

def boostMix(ch, others) {
  def count = others.size() + 1
  ch.then(
    *others,
    singleton: false,
    onNext: { val, i ->
      emit(val)
    },
    onComplete: { i ->
      count -= 1
      if( count == 0 )
        done()
    }
  )
}

@ValueObject
class MultiMapCriteria {
  String name
  Closure transform
}

def boostMultiMap(ch, List<MultiMapCriteria> criteria) {
  def names = criteria.collect { c -> c.name }
  ch.thenMany(emits: names) { val ->
    criteria.each { c ->
      emit(c.name, c.transform(val))
    }
  }
}

def boostReduce(ch, seed=null, Closure accumulator) {
  def result = seed
  ch.then(
    singleton: true,
    onNext: { val ->
      result = result == null ? val : accumulator(result, val)
    },
    onComplete: { emit(result) }
  )
}

def boostTake(ch, int n) {
  def count = 0
  ch.then(singleton: false) { val ->
    if( n != 0 )
      emit(val)
    if( n >= 0 && ++count >= n )
      done()
  }
}

def boostTranspose(ch, by = null, boolean remainder = false) {
  def cols = by == null
    ? []
    : by instanceof List ? by : [by]

  ch.then(singleton: false) { val ->
    if( !(val instanceof List) )
      error "In `transpose` operator -- expected a tuple but received: ${val} [${val.class.simpleName}]"

    def tuple = val as List
    cols.eachWithIndex { col, i ->
      def el = tuple[col]
      if( !(el instanceof List) )
        error "In `transpose` operator -- expected a list at tuple index ${col} but received: ${el} [${el.class.simpleName}]"
    }

    def indices = cols ?: {
      def result = []
      tuple.eachWithIndex { el, i ->
        if( el instanceof List )
          result << i
      }
      result
    }.call()

    def max = indices.collect { i -> tuple[i].size() }.max()

    max.times { i ->
      def result = []

      tuple.eachWithIndex { el, k ->
        if( k in indices ) {
          def list = el
          if( i < list.size() )
            result[k] = list[i]
          else if( remainder )
            result[k] = null
          else
            return
        }
        else
          result[k] = el
      }
      emit(result)
    }
  }
}

def boostUntil(ch, Closure predicate) {
  ch.then { val ->
    if( predicate(val) )
      done()
    else
      emit(val)
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
    onNext: { val ->
      index += 1
      if( index % step == 0 )
        windows << []

      windows.each { window -> window << val }

      def window = windows.head()
      if( window.size() == size ) {
        emit(window)
        windows = windows.tail()
      }
    },
    onComplete: {
      if( remainder && windows.size() > 0 )
        windows.each { window -> emit(window) }
    }
  )
}

def parseQueueValues(String queue) {
  if( queue.contains('..') ) {
    def (min, max) = queue.tokenize('..')
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

workflow {
  params.empty = false
  params.value = null
  params.queue = '1..10'

  ch = params.empty
    ? Channel.empty()
    : params.value
      ? Channel.value( params.value )
      : Channel.fromList( parseQueueValues(params.queue) )

  // branch
  ch_branch = boostBranch(ch, [
      new BranchCriteria('div1', { v -> v % 1 == 0 }),
      new BranchCriteria('div2', { v -> v % 2 == 0 }),
      new BranchCriteria('div3', { v -> v % 3 == 0 }),
    ])

  Channel.empty()
    .mix(
      ch_branch.div1.map { v -> "div1: ${v}" },
      ch_branch.div2.map { v -> "div2: ${v}" },
      ch_branch.div3.map { v -> "div3: ${v}" }
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

  // concat
  ch1 = ch
  ch2 = ch
  ch3 = ch
  boostConcat(ch1, [ch2, ch3])
    .dump(tag: 'concat')

  // cross
  ch_left = ch
  ch_right = ch
  boostCross(ch_left, ch_right)
    .dump(tag: 'cross')

  // cross (by)
  ch_left = ch.map { v -> [v, v.toString()] }
  ch_right = ch.map { v -> [v, v.toString() * asInteger(v)] }
  boostCross(ch_left, ch_right, by: 0)
    .dump(tag: 'cross-by')

  // distinct
  ch_rev = ch | collect | flatMap { v -> v.reverse() }
  boostDistinct(ch.concat(ch_rev))
    .dump(tag: 'distinct')

  // filter
  boostFilter(ch) { v -> v > 5 }
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

  // join
  ch_left = ch.map { v -> [v, v.toString()] }
  ch_right = ch.map { v -> [v, v.toString() * asInteger(v)] }
  boostJoin(ch_left, ch_right)
    .dump(tag: 'join')

  // last
  boostLast(ch)
    .dump(tag: 'last')

  // map
  boostMap(ch) { v -> v * 2 }
    .dump(tag: 'map')

  // merge
  ch1 = ch
  ch2 = ch
  ch3 = Channel.value('foo')
  boostMerge(ch1, [ch2, ch3])
    .dump(tag: 'merge')

  // mix
  ch1 = ch
  ch2 = ch
  ch3 = ch
  boostMix(ch1, [ch2, ch3])
    .dump(tag: 'mix')

  // multiMap
  ch_multi = boostMultiMap(ch, [
      new MultiMapCriteria('mul1', { v -> v * 1 }),
      new MultiMapCriteria('mul2', { v -> v * 2 }),
      new MultiMapCriteria('mul3', { v -> v * 3 }),
    ])

  Channel.empty()
    .mix(
      ch_multi.mul1.map { v -> "mul1: ${v}" },
      ch_multi.mul2.map { v -> "mul2: ${v}" },
      ch_multi.mul3.map { v -> "mul3: ${v}" }
    )
    .dump(tag: 'multiMap')

  ch_multi.mul1.dump(tag: 'multiMap:mul1')
  ch_multi.mul2.dump(tag: 'multiMap:mul2')
  ch_multi.mul3.dump(tag: 'multiMap:mul3')

  // reduce
  boostReduce(ch) { acc, v -> acc + v }
    .dump(tag: 'reduce')

  // take
  boostTake(ch, 3)
    .dump(tag: 'take')

  // transpose
  ch_grouped = ch.map { v -> [v, 1..v as ArrayList] }
  boostTranspose( ch_grouped.dump(tag: 'transpose') )
    .dump(tag: 'transpose')

  // until
  boostUntil(ch) { v -> v == 7 }
    .dump(tag: 'until')

  // window
  boostWindow(ch, 3, 1, true)
    .dump(tag: 'window')
}
