
include { then } from 'plugin/nf-boost'

def boostBuffer(ch, int size, boolean remainder=false) {
  if( size <= 0 )
    error 'buffer `size` parameter must be greater than zero'

  def buffer = []
  ch.then([
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
  ])
}

def boostCollect(ch) {
  final result = []
  ch.then(singleton: true, [
    onNext: { it ->
      result << it
    },
    onComplete: {
      if( result )
        emit(result)
    }
  ])
}

def boostFilter(ch, Closure predicate) {
  ch.then { it ->
    if( predicate(it) )
      emit(it)
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

def boostIfEmpty(ch, value) {
  def empty = true
  ch.then([
    onNext: { it ->
      emit(it)
      empty = false
    },
    onComplete: {
      if( empty )
        emit(value)
    }
  ])
}

def boostMap(ch, Closure mapper) {
  ch.then { it ->
    emit(mapper(it))
  }
}

def boostReduce(ch, seed, Closure accumulator) {
  def result = seed
  ch.then(singleton: true, [
    onNext: { it ->
      result = accumulator(result, it)
    },
    onComplete: { emit(result) }
  ])
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
    // TODO: stop immediately if n == 0
    if( n != 0 )
      emit(it)
    if( n >= 0 && ++count >= n )
      done()
  }
}

def boostUnique(ch) {
  def result = []
  ch.then([
    onNext: { it ->
      if( it !in result )
        result << it
    },
    onComplete: {
      result.each( v -> emit(v) )
    }
  ])
}

def parseQueueValues( queue ) {
  if( queue.contains('..') ) {
    final (min, max) = queue.tokenize('..')
    return (min as int) .. (max as int)
  }
  else {
    return queue.tokenize(',')
  }
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

  // buffer
  boostBuffer(ch, 3, true)
    .dump(tag: 'buffer')

  // collect
  boostCollect(ch)
    .dump(tag: 'collect')

  // filter
  boostFilter(ch) { it > 5 }
    .dump(tag: 'filter')

  // flatMap
  boostFlatMap(ch) { [it] * it }
    .dump(tag: 'flatMap')

  // ifEmpty
  boostIfEmpty(ch, 'foo')
    .dump(tag: 'ifEmpty')

  // map
  boostMap(ch) { it * 2 }
    .dump(tag: 'map')

  // reduce
  boostReduce(ch, 0) { acc, v -> acc + v }
    .dump(tag: 'reduce')

  // scan
  boostScan(ch, 0) { acc, v -> acc + v }
    .dump(tag: 'scan')

  // take
  boostTake(ch, 0)
    .dump(tag: 'take')

  // unique
  boostUnique(ch.flatMap { [it] * it })
    .dump(tag: 'unique')
}
