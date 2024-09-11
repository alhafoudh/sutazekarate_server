threads_count = ENV.fetch('PUMA_THREADS') { 5 }.to_i
threads threads_count, threads_count

port ENV.fetch('PORT') { 3000 }

workers ENV.fetch('PUMA_WORKERS') { 1 }.to_i