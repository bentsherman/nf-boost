
params.bam_count = 10
params.bam_size = '100M'
params.sleep_mean = 10
params.sleep_std = 3

rng = new Random()

process BAM1 {
    input:
    val index

    output:
    tuple val(index), path("${index}-1.bam"), emit: bam
    path "${index}-1.log", emit: log

    script:
    """
    sleep ${params.sleep_mean + rng.nextInt() % params.sleep_std}
    dd if=/dev/random of=${index}-1.bam bs=1 count=0 seek=${params.bam_size}
    echo "Wrote ${params.bam_size} to ${index}-1.bam" > ${index}-1.log
    """
}

process BAM2 {
    input:
    tuple val(index), path("${index}-1.bam")

    output:
    tuple val(index), path("${index}-2.bam"), emit: bam
    path "${index}-2.log", emit: log

    script:
    """
    sleep ${params.sleep_mean + rng.nextInt() % params.sleep_std}
    cp ${index}-1.bam ${index}-2.bam
    echo "Copied ${index}-1.bam to ${index}-2.bam" > ${index}-2.log
    """
}

process BAM3 {
    publishDir 'results', pattern: '*.bam'

    input:
    tuple val(index), path("${index}-2.bam")

    output:
    tuple val(index), path("${index}-3.bam"), emit: bam
    path "${index}-3.log", emit: log

    script:
    """
    sleep ${params.sleep_mean + rng.nextInt() % params.sleep_std}
    cp ${index}-2.bam ${index}-3.bam
    echo "Copied ${index}-2.bam to ${index}-3.bam" > ${index}-3.log
    """
}

process SUMMARY {
    publishDir 'results'

    input:
    path logs

    output:
    path 'summary.txt'

    script:
    """
    sleep ${params.sleep_mean + rng.nextInt() % params.sleep_std}
    cat ${logs} > summary.txt
    """
}

workflow {
    BAM1( Channel.of( 1..params.bam_count ) )
    BAM2(BAM1.out.bam)
    BAM3(BAM2.out.bam)

    BAM1.out.log
    | mix(BAM2.out.log, BAM3.out.log)
    | collect
    | SUMMARY
}
