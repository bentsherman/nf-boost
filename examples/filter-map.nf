
include { filterMap } from 'plugin/nf-boost'

workflow {
  Channel.fromList( 1..15 )
    .filterMap { n ->
      if( n % 15 == 0 )
        return Optional.of("${n}: FizzBuzz")
      if( n % 3 == 0 )
        return Optional.of("${n}: Fizz")
      if( n % 5 == 0 )
        return Optional.of("${n}: Buzz")
      return Optional.empty()
    }
    .view()
}
