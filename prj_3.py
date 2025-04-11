import pandas as pd
df = pd.read_csv("/home/han/DE_K13/Prj_3/tmdb-movies.csv")

# Sắp xếp theo ngày phát hành giảm dần và lưu file mới
df['release_date'] = pd.to_datetime(df['release_date'], errors = 'coerce')
sorted_by_date = df.sort_values(by = "release_date", acending = False)
sorted_by_date.to_csv("movies_sorted_by_release_date.csv", index=False)

# Lọc phim có đánh giá trung bình trên 7.5
high_rated = df[df['vote_average'] > 7.5]
high_rated.to_csv("high_rated_movies.csv", index=False)

# Tìm phim có doanh thu cao nhất và thấp nhất
max_revenue_movie = df.loc[df['revenue'].idxmax()]
min_revenue_movie = df.loc[df['revenue'].idxmin()]

#Tính tổng doanh thu tất cả các phim
total_revenue = df['revenue'].sum()

# Top 10 phim có lợi nhuận cao nhất
df['profit'] = df['revenue'] - df['budget']
top_profit_movies = df.sort_values(by='profit', ascending=False).head(10)

# Đạo diễn có nhiều phim nhất & Diễn viên đóng nhiều phim nhất
df['director'].value_counts().idxmax()

from collections import Counter
actor_counts = Counter()
df['cast'].dropna().apply(lambda x: actor_counts.update(x.split('|')))
most_common_actor = actor_counts.most_common(1)[0]

# Thống kê số lượng phim theo từng thể loại
genre_counts = Counter()
df['genres'].dropna().apply(lambda x: genre_counts.update(x.split('|')))
genre_df = pd.DataFrame(genre_counts.items(), columns=['Genre', 'Count']).sort_values(by='Count', ascending=False)
