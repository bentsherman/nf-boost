
include { scan } from 'plugin/nf-boost'

workflow {
  channel.fromList( 1..10 )
    .scan { acc, v -> acc + v }
    .view()
}
