reset
set datafile separator ","
# set title "Delta-based synchronization"
set xlabel "Number of elements in the SOR-Set"
set xrange [1000000:27000000]
set xtics ("1M" 1000000, "5M" 5000000, "10M" 10000000, "15M" 15000000, "20M" 20000000, "25M" 25000000) nomirror out scale 1
set ylabel "Average merge time (s) for 1M updates"
set key top right noautotitle
set terminal pdf enhanced color solid
set output "bench_delta.pdf"

plot 'bench_delta.csv' every 3::1 using 1:($10/1000) with points linewidth 2 linecolor rgb "#ff0000",\
     'bench_delta.csv' every 3::1 using 1:($11/1000) with points linewidth 2 linecolor rgb "#62b462",\
     'bench_delta.csv' every 3::1 using 1:($12/1000) with points linewidth 2 linecolor rgb "#ff7f2a",\
     'bench_delta.csv' every 3::1 using 1:($13/1000) with points linewidth 2 linecolor rgb "#6aa5df",\
     'bench_delta.csv' every ::1 using 1:($10/1000) with lines linewidth 2 linecolor rgb "#ff0000" smooth bezier title "Total",\
     'bench_delta.csv' every ::1 using 1:($11/1000) with lines linewidth 2 linecolor rgb "#62b462" smooth bezier title "Get ids pages",\
     'bench_delta.csv' every ::1 using 1:($12/1000) with lines linewidth 2 linecolor rgb "#ff7f2a" smooth bezier title "Get update elements",\
     'bench_delta.csv' every ::1 using 1:($13/1000) with lines linewidth 2 linecolor rgb "#6aa5df" smooth bezier title "Add updates"

set output
reset