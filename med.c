/*
  Copyright (C) 2013, 2016 Luchen Tan and Charles L. A. Clarke

  Compute maximized effectiveness differences.

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <math.h>
#include <ctype.h>
#include <assert.h>
#include <unistd.h>
#include <errno.h>

char *version = "Tue May 31 13:33:54 EDT 2016";

/* Many of the constants below should really be set by command line args.  */

/* G: Mamimum relevance grade. Must be positive. */
#define G 2
/* rp: global array of relevance probabilities, indexed by relevance grade. */
double rp[G + 1];

/* DEPTH: Max depth for all measures. */
#define DEPTH 1000 

/* NDCG_DEPTH: Depth for computing NDCG. */
#define NDCG_DEPTH 20

/* PSI: User persistence value for RBP. */
#define PSI 0.95
/* RBP_DEPTH: Depth for computing RBP (as deep as possible). */
#define RBP_DEPTH DEPTH

/* P: Number of relevant documents for brute force ERR maximization. */
#define P 5
/* ERR_DEPTH: Depth for brute force ERR maximization. */
#define ERR_DEPTH 30

/* A natural number that's large enough. */
#define LARGE_ENOUGH 1000000

/*
  A bunch of my standard C utility functions are cloned below.  This stuff
  shouldn't be in this file, but we wanted to distribute a single-file program.
  Skip down to the "naturalNumber" function.
*/

static char *programName = (char *) 0;

static void
error (char *format, ...)
{
  va_list args;

  fflush (stderr);
  if (programName)
    fprintf (stderr, "%s: ", programName);
  va_start (args, format);
  vfprintf (stderr, format, args);
  va_end (args);
  fflush (stderr);
  exit (1);
}

static void *
localMalloc (size_t size)
{
  void *memory;

  if ((memory = malloc (size)))
    return memory;
  else
    {
      error ("Out of memory!\n");
      /*NOTREACHED*/
      return (void *) 0;
    }
}

static void *
localRealloc (void *memory, size_t size)
{
  if ((memory = realloc (memory, size)))
    return memory;
  else
    {
      error ("Out of memory!\n");
      /*NOTREACHED*/
      return (void *) 0;
    }
}

static void *
localFree (void *memory)
{
  if (memory)
    free (memory);
  return (void *) 0;
}

static char *
localStrdup (const char *string)
{
  return strcpy (localMalloc (strlen (string) + 1), string);
}

static void
setProgramName (char *argv0)
{
  char *pn;

  if (programName)
    localFree (programName);

  if (argv0 == (char *) 0)
    {
      programName = (char *) 0;
      return;
    }

  for (pn = argv0 + strlen (argv0); pn > argv0; --pn)
    if (*pn == '/')
      {
        pn++;
        break;
      }

  programName = localStrdup (pn);
}

static char *
getProgramName (void)
{
  return programName;
}

#define GETLINE_INITIAL_BUFSIZ 256

static char *
getLine (FILE *fp)
{
  static unsigned bufsiz = 0;
  static char *buffer = (char *) 0;
  unsigned count = 0;

  if (bufsiz == 0)
    {
      buffer = (char *) localMalloc ((unsigned) GETLINE_INITIAL_BUFSIZ);
      bufsiz = GETLINE_INITIAL_BUFSIZ;
    }

  if (fgets (buffer, bufsiz, fp) == NULL)
    return (char *) 0;

  for (;;)
    {
      unsigned nlpos = strlen (buffer + count) - 1;
      if (buffer[nlpos + count] == '\n')
        {
          if (nlpos && buffer[nlpos + count - 1] == '\r')
            --nlpos;
          buffer[nlpos + count] = '\0';
          return buffer;
        }
      count = bufsiz - 1;
      bufsiz <<= 1;
      buffer = (char *) localRealloc (buffer, (unsigned) bufsiz);
      if (fgets (buffer + count, count + 2, fp) == NULL)
        {
          buffer[count] = '\0';
          return buffer;
        }
    }
}

static int
split (char *s, char **a, int m)
{
  int n = 0;

  while (n < m)
    {
      for (; isspace (*s); s++)
        ;
      if (*s == '\0')
        return n;

      a[n++] = s;

      for (s++; *s && !isspace (*s); s++)
        ;
      if (*s == '\0')
        return n;

      *s++ = '\0';
    }

  return n;
}

/* Code specific to this program starts here. */

/* naturalNumber:
        Careful parsing for topic numbers and ranks.
*/
static int
naturalNumber (char *s)
{
  int value = 0;

  if (s == (char *) 0 || *s == '\0')
    return -1;

  for (; *s; s++)
    if (*s >= '0' && *s <= '9')
      {
        if (value > LARGE_ENOUGH)
          return -1;
        value = 10*value + (*s - '0');
      }
    else
      return -1;

  return value;
}

/* struct result:
        Information for given docno for a given topic for a given runid.
        rank  = rank in this run
        rankx = rank in run we're comparing against
        rel   = currently assigned relevance value
        score = score for traditional TREC sorting
*/
struct result {
  char *docno, *runid;
  int topic, rank, rankx, rel;
  double score;
};

/* struct qrel:
        Relevance information for a topic/docno pair.
*/
struct qrel {
  char *docno;
  int topic, rel;
};

/* dumpResults:
    Dump an array of result structures (strictly for debugging).
*/
static void
dumpResults (struct result *r, int size)
{
  int i;

  for (i = 0; i < size; i++)
    printf (
      "%s %d %d %d %d %s\n",
      r[i].runid, r[i].topic, r[i].rank, r[i].rankx, r[i].rel, r[i].docno
    );
}

/* resultCompareByRank:
     qsort comparison funtion for results; sort by topic and then by rank
*/
static int
resultCompareByRank (const void *a, const void *b)
{
  struct result *ar = (struct result *) a;
  struct result *br = (struct result *) b;
  if (ar->topic < br->topic)
    return -1;
  if (ar->topic > br->topic)
    return 1;
  return ar->rank - br->rank;
}

/* resultSortByRank:
     sort results, first by topic and then by rank
*/
static void
resultSortByRank (struct result *list, int results)
{
  qsort (list, results, sizeof (struct result), resultCompareByRank);
}


/* resultCompareByDocno:
     qsort comparison funtion for results; sort by topic and then by docno
*/
static int
resultCompareByDocno (const void *a, const void *b)
{
  struct result *ar = (struct result *) a;
  struct result *br = (struct result *) b;
  if (ar->topic < br->topic)
    return -1;
  if (ar->topic > br->topic)
    return 1;
  return strcmp(ar->docno, br->docno);
}

/* resultSortByDocno:
     sort results, first by topic and then by docno
*/
static void
resultSortByDocno (struct result *list, int results)
{
  qsort (list, results, sizeof (struct result), resultCompareByDocno);
}

/* resultCompareByScore:
     qsort comparison function for results; sort by topic, then by score, and
     then by docno (which is the traditional sort order for TREC runs)
*/
static int
resultCompareByScore (const void *a, const void *b)
{
  struct result *ar = (struct result *) a;
  struct result *br = (struct result *) b;
  if (ar->topic < br->topic)
    return -1;
  if (ar->topic > br->topic)
    return 1;
  if (ar->score < br->score)
    return 1;
  if (ar->score > br->score)
    return -1;
  return strcmp (br->docno, ar->docno);
}

/* resultSortByScore:
     sort results; first by topic, then by score, and then by docno
*/
static void
resultSortByScore (struct result *list, int results)
{
  qsort (list, results, sizeof (struct result), resultCompareByScore);
}

/* compareQ:
     qsort comparison funtion for qrels; sort by topic and then by docno
*/
static int
compareQ (const void *a, const void *b)
{
  struct qrel *aq = (struct qrel *) a;
  struct qrel *bq = (struct qrel *) b;
  if (aq->topic < bq->topic)
    return -1;
  if (aq->topic > bq->topic)
    return 1;
  return strcmp(aq->docno, bq->docno);
}

/* sortQ:
     sort qrels, first by topic and then by docno
*/
static void
sortQ (struct qrel *q, int qrels)
{
  qsort (q, qrels, sizeof (struct qrel), compareQ);
}

/* forceTraditionalRanks:
     Re-assign ranks so that runs are sorted by score and then by docno,
     which is the traditional sort order for TREC runs.
*/
static void
forceTraditionalRanks (struct result *r, int n)
{
  int i, rank, currentTopic = -1;

  resultSortByScore (r, n);

  for (i = 0; i < n; i++)
    {
      if (r[i].topic != currentTopic)
        {
          currentTopic = r[i].topic;
          rank = 1;
        }
      r[i].rank = rank;
      rank++;
    }
}

/* applyCutoff:
     Throw away results deeper than a specified depth.
     Run must be sorted by topic and then rank.
     Return number of remaing results.
*/
static int
applyCutoff (struct result *r, int n, int depthMax)
{
  int i, j, depth, currentTopic = -1;

  j = 0;
  for (i = 0; i < n; i++)
    {
      if (r[i].topic != currentTopic)
        {
          currentTopic = r[i].topic;
          depth = 1;
        }
      else
        depth++;
      if (depth <= depthMax)
        r[j++] = r[i];
    }

  return j;
}

/* loadRun:
        load a run from a named file; perform initial cleaning and sorting
*/
static struct result *
loadRun (char *run, int *size)
{
  FILE *fp;
  char *line, *runid;
  int i = 0, n = 0, needRunID = 1;
  struct result *r;

  if ((fp = fopen (run, "r")) == NULL)
    error ("cannot open run file \"%s\"\n", run);

  while (getLine (fp))
    n++;

  fclose (fp);

  if (n == 0)
    error ("run file \"%s\" is empty\n", run);

  r = localMalloc (n*sizeof (struct result));

  if ((fp = fopen (run, "r")) == NULL)
    error ("cannot open run file \"%s\"\n", run);

  while ((line = getLine (fp)))
    {
      char *a[6];
      int topic, rank;

      if (
        split (line, a, 6) != 6
        || (topic = naturalNumber (a[0])) < 0
        || (rank = naturalNumber (a[3])) < 0
      )
        error ("syntax error in run file \"%s\" at line %d\n", run, i + 1);
      else
        {
         if (needRunID)
            {
              runid = localStrdup (a[5]);
              needRunID = 0;
            }
          r[i].docno = localStrdup (a[2]);
          r[i].runid = runid;
          r[i].topic = topic;
          r[i].rank = rank;
          r[i].rankx = -1;
          r[i].rel = -1;
          sscanf (a[4],"%lf", &(r[i].score));
          i++;
        }
    }

  /* force ranks to be consistent with traditional TREC sort order */
  forceTraditionalRanks (r, n);

  /*
    apply depth cutoff
    (Why am I doing this work, if I now have a per-measure depth?)
  */
  n = applyCutoff (r, n, DEPTH);


  /* for each topic, verify that docnos have not been duplicated */
  resultSortByDocno (r, n);
  for (i = 1; i < n; i++)
    if (r[i].topic == r[i-1].topic && strcmp(r[i].docno,r[i-1].docno) == 0)
      error (
        "duplicate docno (%s) for topic %d in run file \"%s\"\n",
        r[i].docno, r[i].topic, run
      );

  *size = n;
  return r;
}

/* loadQ:
        load qrels from a named file; sort by topic then docno
*/
static struct qrel *
loadQ (char *qrels, int *size)
{
  FILE *fp;
  char *line;
  struct qrel *q;
  int i = 0, n = 0;

  if ((fp = fopen (qrels, "r")) == NULL)
    error ("cannot open qrels file \"%s\"\n", qrels);

  while (getLine (fp))
    n++;

  fclose (fp);

  if (n == 0)
    error ("qrel file \"%s\" is empty\n", qrels);

  q = localMalloc (n*sizeof (struct qrel));

  if ((fp = fopen (qrels, "r")) == NULL)
    error ("cannot open qrels file \"%s\"\n", qrels);

  while ((line = getLine (fp)))
    {
      char *a[4];
      int topic, rel;

      if (
        split (line, a, 4) != 4
        || (topic = naturalNumber (a[0])) < 0
        || (rel = naturalNumber (a[3])) < 0
      )
        error ("syntax error in qrel file \"%s\" at line %d\n", qrels, i + 1);
      else
        {
          q[i].docno = localStrdup (a[2]);
          q[i].topic = topic;
          if (rel > G)
            rel = G;
          q[i].rel = rel;
          i++;
        }
    }


  /* for each topic, verify that docnos have not been duplicated */
  sortQ (q, n);
  for (i = 1; i < n; i++)
    if (q[i].topic == q[i-1].topic && strcmp(q[i].docno,q[i-1].docno) == 0)
      error (
        "duplicate docno (%s) for topic %d in qrels file \"%s\"\n",
        q[i].docno, q[i].topic, qrels
      );

  *size = n;
  return q;
}

/* labelQ:
        label a run with pre-determined relevance values
*/
static void
labelQ (struct result *r, int results, struct qrel *q, int qrels)
{
  int i = 0, j = 0;

  while (i < results && j < qrels)
    if (r[i].topic < q[j].topic)
      i++;
    else if (r[i].topic > q[j].topic)
      j++;
    else
      {
        int cmp = strcmp (r[i].docno, q[j].docno);

        if (cmp < 0)
          i++;
        else if (cmp > 0)
          j++;
        else
          {
            r[i].rel = q[j].rel;
            i++;
            j++;
          }
      }
}

/* crossLabelRun:
        record rank information across runs
*/
static void
crossLabelRuns (struct result *r1, int size1, struct result *r2, int size2)
{
  int i = 0, j = 0;
  
  while (i < size1 && j < size2)
    if (r1[i].topic < r2[j].topic)
      i++;
    else if (r1[i].topic > r2[j].topic)
      j++;
    else
      {
        int cmp = strcmp (r1[i].docno, r2[j].docno);

        if (cmp < 0)
          i++;
        else if (cmp > 0)
          j++;
        else
          {
            r1[i].rankx = r2[j].rank;
            r2[j].rankx = r1[i].rank;
            i++;
            j++;
          }
      }
}

/* nextTopicSize:
        number of results for the next topic in the result list
*/
static int
nextTopicSize (struct result *r, int size, int *topic)
{
  int i;

  if (size == 0 || r == (struct result *) 0)
    return 0;

  *topic = r[0].topic;

  for (i = 0; i < size && r[i].topic == r[0].topic; i++)
    ;

  return i;
}

/* errCompute:
        Compute ERR over a result list applying the given relevance grade for
        to variables and assuming bound variables have zero relvance.
*/
static double
errCompute (struct result *r, int size, int sizex, int relFree)
{
  int i;
  double g = 1.0, score = 0.0;

  for (i = 0; i < size; i++)
    {
      double rp0;

      if (r[i].rel >= 0)
        rp0 = rp[r[i].rel]; /* predetermined (status may be temporary) */
      else if (r[i].rankx > 0 && r[i].rankx <= sizex)
        rp0 = 0.0; /* bound variable */
      else
        rp0 = rp[relFree]; /* free variable */

      score += g*rp0/r[i].rank;
      g *= (1 - rp0);
    }

  return score;
}

/* errComputeDiff:
  	Compute the maximized ERR difference between two result lists
        (under some assumptions).
*/
static double
errComputeDiff (struct result *r, int size, struct result *rx, int sizex)
{
  double score = errCompute (r, size, sizex, G);
  double scorex = errCompute (rx, sizex, size, 0);

  return fabs(score - scorex);
}

/* errHalf:
        Compute ERR difference between one result list and another.
        Up to p bound variables may be set starting at a given depth.
*/

static double
errHalf (
  struct result *r, int size, struct result *rx, int sizex, int p, int start
)
{
  int i;
  double x, max = errComputeDiff (r, size, rx, sizex);

  if (p <= 0) return max;

  for (i = start; i < size; i++)
    if (r[i].rel == -1)
      {
        if (r[i].rankx > 0 && r[i].rankx <= sizex) /* bound */
          {
            r[i].rel = rx[r[i].rankx - 1].rel = G; /* let's pretend */
            x = errHalf (r, size, rx, sizex, p - 1, i + 1);
            if (max < x) max = x;
            r[i].rel = rx[r[i].rankx - 1].rel = -1;
          }
        else
          {
            r[i].rel = G; /* let's say */
            x = errHalf (r, size, rx, sizex, p - 1, i + 1);
            if (max < x) max = x;
            r[i].rel = -1;
            return max;  /* it won't help to go deeper */
          }
      }

  return max;
}

static double
errMaximize (struct result *r1, int size1, struct result *r2, int size2)
{
  double max1, max2;

  if (size1 > ERR_DEPTH) size1 = ERR_DEPTH;
  if (size2 > ERR_DEPTH) size2 = ERR_DEPTH;

  max1 = errHalf (r1, size1, r2, size2, P, 0);
  max2 = errHalf (r2, size2, r1, size1, P, 0);

  return (max1 > max2 ? max1 : max2);
}

static double
rbpHalf (struct result *r, int size, int sizex, double *predetermined)
{
  int i;
  double pre = 0.0, max = 0.0;

  /* RBP uses binary relevance.  We interpret rel > 0 as relevant. */
  for (i = 0; i < size; i++)
    if (r[i].rel == -1)
      {
        if (r[i].rankx == -1)
          max += pow(PSI,r[i].rank - 1);
        else if (r[i].rank < r[i].rankx)
          if (r[i].rankx < sizex)
            max += (pow(PSI,r[i].rank - 1) - pow(PSI, r[i].rankx - 1));
          else
            max += pow(PSI,r[i].rank - 1);

      }
    else if (r[i].rel > 0)
      pre += pow(PSI,r[i].rank - 1);

  /* To infinity and beyond!  (Only matters if size is small.) */
  max += pow(PSI,size)/(1 - PSI);

  *predetermined = pre;
  return max;
}

static double
rbpMaximize (struct result *r1, int size1, struct result *r2, int size2)
{
  double pre1, pre2, max1, max2;

  size1 = (size1 > RBP_DEPTH ? RBP_DEPTH : size1);
  size2 = (size2 > RBP_DEPTH ? RBP_DEPTH : size2);
  max1 = rbpHalf (r1, size1, size2, &pre1);
  max2 = rbpHalf (r2, size2, size1, &pre2);
  max1 += pre1 - pre2;
  max2 += pre2 - pre1;

  return (1.0 - PSI)*(max1 > max2 ? max1 : max2);
}

static double
ndcgNorm (int k)
{
  int i;
  double norm = 0.0;

  for (i = 1; i <= k; i++)
    norm += rp[G]/log2(i + 1);

  return norm;
}

static double
ndcgHalf (struct result *r, int size, int sizex, double *predetermined)
{
  int i;
  double pre = 0.0, max = 0.0;
  double norm = 0.0;

  for (i = 0; i < size; i++)
    {
      double discount = 1.0/log2((double) r[i].rank + 1);

      if (r[i].rel == -1)
        {
          if (r[i].rankx == -1)
            max += rp[G]*discount;
          else if (r[i].rank < r[i].rankx)
            if (r[i].rankx < sizex)
              max += rp[G]*(discount - 1.0/log2((double) r[i].rankx + 1));
            else
              max += rp[G]*discount;
        }
      else if (r[i].rel > 0)
        pre += rp[r[i].rel]*discount;
    }

  *predetermined = pre;
  return max;
}

static double
ndcgMaximize (struct result *r1, int size1, struct result *r2, int size2)
{
  double pre1, pre2, max1, max2;
  double norm = ndcgNorm (NDCG_DEPTH);

  size1 = (size1 > NDCG_DEPTH ? NDCG_DEPTH : size1);
  size2 = (size2 > NDCG_DEPTH ? NDCG_DEPTH : size2);
  max1 = ndcgHalf (r1, size1, size2, &pre1);
  max2 = ndcgHalf (r2, size2, size1, &pre2);
  max1 += pre1 - pre2;
  max2 += pre2 - pre1;

  return (max1 > max2 ? max1 : max2)/norm;
}

static void
med (char *run1, char *run2, char *qrels)
{
  int i, j, n = 0;
  int topic1, topic2, size1, size2;
  struct result *r1, *r2;
  char *runid1, *runid2;
  double err_max = 0.0, err_tot = 0.0;
  double rbp_max = 0.0, rbp_tot = 0.0;
  double ndcg_max = 0.0, ndcg_tot = 0.0;

  printf ("run1,run2,topic,MED-nDCG@%d,MED-RBP,MED-ERR\n", NDCG_DEPTH);

  r1 = loadRun (run1, &size1);
  runid1 = r1[0].runid;
  r2 = loadRun (run2, &size2);
  runid2 = r2[0].runid;

  if (size1 <= 0 || size2 <= 0)
    {
      printf ("%s,%s,0.0,0.0,0.0\n", runid1, runid2);
      return;
    }

  if (qrels)
    {
      struct qrel *q;
      int sizeQ;

      q = loadQ (qrels, &sizeQ);
      labelQ (r1, size1, q, sizeQ);
      labelQ (r2, size2, q, sizeQ);
    }

  crossLabelRuns (r1, size1, r2, size2);

  /* sort results into rank order. */
  resultSortByRank (r1, size1);
  resultSortByRank (r2, size2);

  while (size1 > 0 && size2 > 0)
    {
      i = nextTopicSize (r1, size1, &topic1);
      j = nextTopicSize (r2, size2, &topic2);
      if (topic1 < topic2)
        {
          r1 += i;
          size1 -= i;
        }
      else if (topic1 > topic2)
        {
          r2 += j;
          size2 -= j;
        }
      else
        {
          ndcg_max = ndcgMaximize (r1, i, r2, j);
          ndcg_tot += ndcg_max;
          rbp_max = rbpMaximize (r1, i, r2, j);
          rbp_tot += rbp_max;
          err_max = errMaximize (r1, i, r2, j);
          err_tot += err_max;
          printf (
            "%s,%s,%d,%.5f,%.5f,%.5f\n",
            runid1, runid2, topic1, ndcg_max, rbp_max, err_max
          );
          n++;
          r1 += i;
          size1 -= i;
          r2 += j;
          size2 -= j;
        }
    }

    if (n > 0)
      printf (
        "%s,%s,amean,%.5f,%.5f,%.5f\n",
        runid1, runid2, ndcg_tot/n, rbp_tot/n, err_tot/n
      );
    else
      printf ("%s,%s,amean,0.00000,0.00000,0.00000\n", runid1, runid2);
}

static void
computeRelevanceProbabilities ()
{
  int i;
  double x = 1.0, y = 1.0;

  for (i = 0; i < G; i++)
    x *= 2.0;

  for (i = 0; i <= G; i++)
    {
      rp[i] = (y - 1.0)/x;
      y *= 2.0;
    }
}

static void
usage ()
{
  error ("Usage: %s run1 run2 [qrels]\n", getProgramName());
}

int
main (int argc, char **argv)
{
  char *run1, *run2, *qrels = (char *) 0;

  setProgramName (argv[0]);

  if (argc == 3 || argc == 4)
    {
      run1 = argv[1];
      run2 = argv[2];
      if (argc == 4)
        qrels= argv[3];
    }
  else
    usage ();

  computeRelevanceProbabilities ();
  med (run1, run2, qrels);

  return 0;
}
