
include { exec ; mergeCsv } from 'plugin/nf-boost'


workflow {
  Channel.of( 1..10 )
    .map { i -> ['id': i, 'name': "record_${i}"] }
    .collect()
    .exec('RECORDS_TO_CSV') { records ->
      def csv = task.workDir.resolve('records.txt')
      mergeCsv(records, csv, sep: '\t')
      return csv
    }
    .view { csv -> csv.text }
}
