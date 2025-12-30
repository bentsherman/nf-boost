
include { mergeCsv } from 'plugin/nf-boost'


process RECORDS_TO_CSV {
  publishDir 'results'

  input:
  val records

  output:
  path 'records.txt'

  exec:
  def path = task.workDir.resolve('records.txt')
  mergeCsv(records, path, sep: '\t')
}


workflow {
  ch_records = channel.of( 1..10 )
    .map { i -> ['id': i, 'name': "record_${i}"] }
    .collect()

  ch_csv = RECORDS_TO_CSV(ch_records)

  ch_csv.view { csv -> csv.text }
}
