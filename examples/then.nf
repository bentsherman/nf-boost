
include { then } from 'plugin/nf-boost'

def boostCollect(ch) {
  def result = []
  ch.then(
    singleton: true,
    onNext: { val ->
      result << val
    },
    onComplete: {
      emit(result)
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
        rightValues[keys].each { rval -> emit( keys + values + [rval] ) }
        leftValues[keys] << values
      }
      else if( i == 1 ) {
        leftValues[keys].each { lval -> emit( keys + [lval] + values ) }
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

def boostFilter(ch, Closure predicate) {
  ch.then { val ->
    if( predicate(val) )
      emit(val)
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
        emit( keys + buffers[0] + buffers[1] )
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

def boostMap(ch, Closure mapper) {
  ch.then { val ->
    emit(mapper(val))
  }
}

def boostMix(ch, others) {
  def count = others.size() + 1
  ch.then(
    others,
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

def boostUntil(ch, Closure predicate) {
  ch.then { val ->
    if( predicate(val) )
      done()
    else
      emit(val)
  }
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

  // collect
  boostCollect(ch)
    .dump(tag: 'collect')

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

  // filter
  boostFilter(ch) { v -> v > 5 }
    .dump(tag: 'filter')

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

  // map
  boostMap(ch) { v -> v * 2 }
    .dump(tag: 'map')

  // mix
  ch1 = ch
  ch2 = ch
  ch3 = ch
  boostMix(ch1, [ch2, ch3])
    .dump(tag: 'mix')

  // reduce
  boostReduce(ch) { acc, v -> acc + v }
    .dump(tag: 'reduce')

  // until
  boostUntil(ch) { v -> v == 7 }
    .dump(tag: 'until')
}
