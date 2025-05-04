# Large Scale Data Processing Final Project - Correlation Clustering

Team: Caden Parajuli

## Algoritm

To compute a clustering, I used the parallel PIVOT algorithm that we discussed in class. This algorithm produces a 3-approximation in $O(\log^2 n)$ time. This algorithm is extremely scalable, and was able to run without issue on all given datasets in a reasonable amount of time. In fact, in test runs it sometimes even took longer to compute the number of disagreements in a given clustering than it did to compute the clustering itself with the PIVOT algorithm.

However, PIVOT is a randomized algorithm, and although it does produce a 3-approximation in expectation, any given run may have more or less than 3 times the optimal number of disagreements. To attempt to find the best possible PIVOT approximation, we compute many PIVOT approximations, and take the best of these approximations. This gives modest theoretical guarantees, and reletively good heuristic results. To further improve heuristics, we not only store the best PIVOT result, but rather the score of every PIVOT result. To implement this, the PIVOT implementation takes a seed and executes deterministically, but pseudorandomly based on the seed. This allows us to create a mapping of seeds to resulting disagreements with $O(r)$ additional storage, where $r$ is the number of PIVOT approximations we compute. 

### Theoretical Guarantees

The main theoretical guarantee is that the PIVOT algorithm outputs a 3-approximation in $O(\log^2 n)$ time (in parallel). This result was covered in class, following the analysis of [Blelloch et al. (2012)](https://doi.org/10.48550/arXiv.1202.3205), who (interestingly enough) used the algorithm for MIS. The algorithm also uses $O(n)$ memory, since the algorithm only uses constant memory per iteration, and this memory is dropped at the end of each iteration (except for the graph itself and the array of clusterings, which use $O(n)$ memory).

The approach of computing the best of multiple PIVOT runs *does* produce a theoretical guarantee, but only to show that our result will not be much worse than a 3-approximation, and we do not get a theoretical improvement on how good our result could be. To obtain this guarantee, we use Markov's inequality. Let $D_s$ be the number of disagreements of a PIVOT output with seed $s$, and let $D_{opt}$ be the number of disagreements of an optimal clustering. Then since $E[D] = 3D_{opt}$, by Markov's inequality we have,

$$
  Pr\left(D \geq a D_{opt}\right) \leq \frac{3}{a}
$$

Now let $D^*$ be the least number of disagreements of $k$ PIVOT trials with seeds $s_1,\dots,s_k$. Then,

$$
  Pr\left( D^* \geq a D_{opt} \right) = Pr\left( \bigwedge_{i=1}^k D_i \geq a D_{opt} \right) \leq {\left(\frac{3}{a}\right)}^k
$$

Thus with a high number of trials, we get a very good theoretical guarantee that our result is not much worse than 3 times the optimal number of disagreements. For instance, with just 10 trials we have

$$
  Pr\left( D^* \geq 4 D_{opt} \right) \leq 0.057
$$

and with 50 trials we have

$$
  Pr\left( D^* \geq 3.2 D_{opt} \right) \leq 0.0397
$$


### Heuristics

We can do better with heuristics to get an idea of how *good* our approximation might actually be. One way we can do this is by looking at not just our bet PIVOT result, but using all of them to compute a mean. Since PIVOT produces at least a 3-approximation in expectation, we can use the mean to get an approximate upper bound on the optimal number of disagreements, and we can judge our best solution based on this, computing an estimated maximum approximation ratio (EMAR). More formally, if our best PIVOT solution is $D^*$ and the mean disagreements across all PIVOT trials is $D_{\mu}$, we define $EMAR(D^*, D_{\mu}) = \frac{3 D^*}{D_{\mu}}$, an estimated upper bound on our approximation ratio. If our graph is not adversarial and is such that PIVOT does better than a 3-approximation in expectation, our actual approximation ratio could be much better than this. The EMAR for PIVOT results is included in the results section below. Note that the EMAR is significantly better for the more dense datasets.


## Results

The PIVOT results are summarized in the table below. Note that the total elapsed time includes the time to compute the number of disagreements for each PIVOT clustering.

|       Dataset               | Disagreements |      Seed     | PIVOT trials | Total elapsed time (s) | Average disagreements | EMAR  |
| --------------------------- | ------------- | ------------  | ------------ | ---------------------- | --------------------- | ----- |
| log_normal_100.csv          |       1980    |     566607675 |    1589      |         1478.34        |           2282        | 2.603 |
| musae_ENGB_edges.csv        |      37770    |    1608026579 |     697      |         1078.53        |          45224        | 2.506 |
| soc-pokec-relationships.csv |   29667967    |     431896136 |     400      |        24465.32        |       30124075        | 2.955 |
| soc-LiveJournal1.csv        |   50667243    |    2125632257 |      31      |         4203.13        |       51733854        | 2.938 |
| twitter_original_edges.csv  |   72389925    |    1495037540 |      50      |         9030.91        |       77308284        | 2.809 |
| com-orkut.ungraph.csv       |  158394897    | 1746383182796 |       3      |         6161.24        |      159056244        | 2.988 |

All of these results were obtained on a local machine with the following specifications:

- HP Z640
  - CPUs: 2x Intel Xeon E5-2683 v4 @ 2.1 GHz
    - 16 cores per socket, 2 threads per core (64 threads total)
  - RAM: 32 GB DDR4 ECC RDIMM @ 2400 MHz
  - Storage: 256 GB SATA SSD
  - Operating System: NixOS 25.05 (Linux kernel 6.12.21)

However, due to the higher memory requirements of the `com-orkut.ungraph.csv` dataset especially when computing the number of disagreements, this was run using only 16 cores to decrease data duplication. Alternatively a cluster consisting of multiple devices with a larger total memory pool could be used. 

The clustering results can be found here: [https://drive.google.com/file/d/1ESWyI0_gT0YSvEqumjk49r1WDvBEWtbv/view?usp=sharing](https://drive.google.com/file/d/1ESWyI0_gT0YSvEqumjk49r1WDvBEWtbv/view?usp=sharing)
