elifePipeline {
    node('containers-jenkins-plugin') {
        def commit
        stage 'Checkout', {
            checkout scm
            commit = elifeGitRevision()
        }
        stage 'Build/Test', {
            dockerBuild('validate-article-json', commit)
        }
    }
}
