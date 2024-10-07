
params.bam_count = 10
params.bam_size = '100M'
params.sleep_mean = 10
params.sleep_std = 3

workflow {
  main:
  BAM1( Channel.of( 1..params.bam_count ), params.sleep_mean, params.sleep_std, params.bam_size )
  BAM2(BAM1.out.bam, params.sleep_mean, params.sleep_std)
  BAM3(BAM2.out.bam, params.sleep_mean, params.sleep_std)

  ch_logs = BAM1.out.log
    | mix(BAM2.out.log, BAM3.out.log)
    | collect

  SUMMARY(ch_logs, params.sleep_mean, params.sleep_std)
}

process BAM1 {
  input:
  val index
  val sleep_mean
  val sleep_std
  val bam_size

  output:
  tuple val(index), path("${index}-1.bam"), emit: bam
  path "${index}-1.log"                   , emit: log

  script:
  """
  sleep ${sleep_mean + randomInt() % sleep_std}
  dd if=/dev/random of=${index}-1.bam bs=1 count=0 seek=${bam_size}
  echo "Wrote ${bam_size} to ${index}-1.bam" > ${index}-1.log
  """
}

process BAM2 {
  input:
  tuple val(index), path("${index}-1.bam")
  val sleep_mean
  val sleep_std

  output:
  tuple val(index), path("${index}-2.bam"), emit: bam
  path "${index}-2.log"                   , emit: log

  script:
  """
  sleep ${sleep_mean + randomInt() % sleep_std}
  cp ${index}-1.bam ${index}-2.bam
  echo "Copied ${index}-1.bam to ${index}-2.bam" > ${index}-2.log
  """
}

process BAM3 {
  publishDir 'results'

  input:
  tuple val(index), path("${index}-2.bam")
  val sleep_mean
  val sleep_std

  output:
  tuple val(index), path("${index}-3.bam"), emit: bam
  path "${index}-3.log"                   , emit: log

  script:
  """
  sleep ${sleep_mean + randomInt() % sleep_std}
  cp ${index}-2.bam ${index}-3.bam
  echo "Copied ${index}-2.bam to ${index}-3.bam" > ${index}-3.log
  """
}

process SUMMARY {
  publishDir 'results'

  input:
  path logs
  val sleep_mean
  val sleep_std

  output:
  path 'summary.txt'

  script:
  """
  sleep ${sleep_mean + randomInt() % sleep_std}
  cat ${logs} > summary.txt
  """
}

def randomInt() {
  Math.random() * Integer.MAX_VALUE
}
