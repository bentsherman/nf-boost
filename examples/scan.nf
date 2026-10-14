
include { scan } from 'plugin/nf-boost'

workflow {
  ch_sums = channel.fromList( 1..10 )
    .scan { acc, v -> acc + v }

  ch_sums.view()
}
