reset
set datafile separator ","
# set title "Throughput for set operations"
set key noautotitle top right
set xlabel "Number of elements in the SOR-Set"
set xrange [1000000:10000000]
set xtics ("1M" 1000000, "2.5M" 2500000, "5M" 5000000, "7.5M" 7500000, "10M" 10000000) nomirror out scale 1
set ylabel "Average requests / second"
set terminal pdf enhanced color solid
set output "bench_ops.pdf"

plot 'bench_ops.csv' every ::1 using 1:8 with points linewidth 2 linecolor rgb "#62b462",\
     'bench_ops.csv' every ::1 using 1:9 with points linewidth 2 linecolor rgb "#ff7f2a",\
     'bench_ops.csv' every ::1 using 1:10 with points linewidth 2 linecolor rgb "#6aa5df",\
     'bench_ops.csv' every ::1 using 1:8 with lines linewidth 2 linecolor rgb "#62b462" smooth bezier title "Add",\
     'bench_ops.csv' every ::1 using 1:9 with lines linewidth 2 linecolor rgb "#ff7f2a" smooth bezier title "Lookup",\
     'bench_ops.csv' every ::1 using 1:10 with lines linewidth 2 linecolor rgb "#6aa5df" smooth bezier title "Remove"

# plot 'bench_ops.csv' every ::1 using 1:5 with linespoints linewidth 2 linecolor rgb "#62b462" title "Add",\
#      'bench_ops.csv' every ::1 using 1:6 with linespoints linewidth 2 linecolor rgb "#ff7f2a" title "Lookup",\
#      'bench_ops.csv' every ::1 using 1:7 with linespoints linewidth 2 linecolor rgb "#6aa5df" title "Remove"

set output
reset