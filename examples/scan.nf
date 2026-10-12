
include { scan } from 'plugin/nf-boost'

workflow {
  ch_sums = channel.fromList( 1..10 )
    .scan { acc, v -> acc + v }

  ch_sums.view()
  ch_sums.collect().subscribe { sums ->
    assert sums == [1, 3, 6, 10, 15, 21, 28, 36, 45, 55]
  }
}
