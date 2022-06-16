#include "gtest/gtest.h"
#include <algorithm>
#include <chrono>
#include <iostream>
#include "core/histogram.h"
#include "core/timer.h"
#include "utils.h"
TEST(HistogramTest, TestAddPerformance){
  utils::Histogram histogram_list;
  utils::Timer<std::chrono::milliseconds> timer;
  const int t = 154;
  const int times = 100000;
  double values[t] = {
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    12,
    14,
    16,
    18,
    20,
    25,
    30,
    35,
    40,
    45,
    50,
    60,
    70,
    80,
    90,
    100,
    120,
    140,
    160,
    180,
    200,
    250,
    300,
    350,
    400,
    450,
    500,
    600,
    700,
    800,
    900,
    1000,
    1200,
    1400,
    1600,
    1800,
    2000,
    2500,
    3000,
    3500,
    4000,
    4500,
    5000,
    6000,
    7000,
    8000,
    9000,
    10000,
    12000,
    14000,
    16000,
    18000,
    20000,
    25000,
    30000,
    35000,
    40000,
    45000,
    50000,
    60000,
    70000,
    80000,
    90000,
    100000,
    120000,
    140000,
    160000,
    180000,
    200000,
    250000,
    300000,
    350000,
    400000,
    450000,
    500000,
    600000,
    700000,
    800000,
    900000,
    1000000,
    1200000,
    1400000,
    1600000,
    1800000,
    2000000,
    2500000,
    3000000,
    3500000,
    4000000,
    4500000,
    5000000,
    6000000,
    7000000,
    8000000,
    9000000,
    10000000,
    12000000,
    14000000,
    16000000,
    18000000,
    20000000,
    25000000,
    30000000,
    35000000,
    40000000,
    45000000,
    50000000,
    60000000,
    70000000,
    80000000,
    90000000,
    100000000,
    120000000,
    140000000,
    160000000,
    180000000,
    200000000,
    250000000,
    300000000,
    350000000,
    400000000,
    450000000,
    500000000,
    600000000,
    700000000,
    800000000,
    900000000,
    1000000000,
    1200000000,
    1400000000,
    1600000000,
    1800000000,
    2000000000,
    2500000000.0,
    3000000000.0,
    3500000000.0,
    4000000000.0,
    4500000000.0,
    5000000000.0,
    6000000000.0,
    7000000000.0,
    8000000000.0,
    9000000000.0,
    1e200,
  };

  timer.Start();
  for ( int  n = 0; n < times ; n++){
    for(int i = 0; i < t; i++){
      histogram_list.Add(values[i]);
    }
  }
  double duration = timer.End();
  std::cerr << "add function :duration time :" << duration << std::endl;
  // std::cerr << histogram_list.ToString() << std::endl;
  std::string list1 = histogram_list.ToString();
  histogram_list.Clear();

  timer.Start();
  for ( int  n = 0; n < times ; n++){
    for(int i = 0; i < t; i++){
      histogram_list.Add_Fast(values[i]);
    }
  }
  duration = timer.End();
  std::cerr << "add_fast function: duration time :" << duration << std::endl;
  // std::cerr << histogram_list.ToString() << std::endl;
  std::string list2 = histogram_list.ToString();
  ASSERT_EQ( list1 , list2 );
}

TEST(HistogramTest, TestCorrectness){
  const double value = 1;
  utils::Histogram histogram_list;
  for ( int i = 0; i < 10000; i++ )
    histogram_list.Add_Fast(value);
  // std::cerr << histogram_list.ToString() << std::endl;
  ASSERT_EQ(histogram_list.Median(), 1.0);

}
int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
