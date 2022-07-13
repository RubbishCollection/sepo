#pragma once
#include <concepts>
#include <vector>
#include <algorithm>
#include <cstdlib>
#include <functional>
#include <tuple>
#include <optional>
#include <atomic>
#include <mutex>
#include <stdexcept>
#include <random>

#include <concurrent_unordered_map.h>


namespace sepo {
	template <typename T>
	concept Key = requires(T a, T b) {
		{a == b} -> std::convertible_to<bool>;
		{a != b} -> std::convertible_to<bool>;
		{(a & b) == a} -> std::convertible_to<bool>;
		{(a & b) == b} -> std::convertible_to<bool>;
	};

#pragma region Forward_Declaration

	template <Key K>
	class Cell;

	template <Key K, typename CellType>
		requires requires {std::derived_from<CellType, Cell<K>>; }
	class Rule;

	template <Key K, typename CellType>
		requires requires {std::derived_from<CellType, Cell<K>>; }
	class World;

	template <Key K, typename CellType>
		requires requires {std::derived_from<CellType, Cell<K>>; }
	class Chunk;

#pragma endregion

	struct Coord {
		union { size_t x; size_t width; };
		union {	size_t y; size_t height;};

		std::optional<Coord> left() {
			if (x > 0)
				return Coord{ x - 1, y };
			else
				return std::nullopt;
		}

		std::optional<Coord> right() {
			if (x < SIZE_MAX)
				return Coord{ x + 1, y };
			else
				return std::nullopt;
		}

		std::optional<Coord> under() {
			if (y < SIZE_MAX)
				return Coord{ x, y + 1 };
			else
				return std::nullopt;
		}

		std::optional<Coord> upper() {
			if (y > 0)
				return Coord{ x, y - 1 };
			else
				return std::nullopt;
		}

		bool operator==(const Coord& other) const {
			return x == other.x && y == other.y;
		}

		struct Hasher {
			size_t operator()(const Coord& coord) const {
				return (std::hash<size_t>()(coord.x) * 0x1f1f1f1f) ^ std::hash<size_t>()(coord.y);
			}
		};
	};

	

	template <Key K>
	class Cell {

	public:
		K key;
		const static bool commit_state_change = false;

		Cell(K key) : key(key) {}

		virtual bool is_empty() const { return !(key & key); }
		virtual void commit_changes() {};
	};

	template <Key K, typename CellType>
		requires requires {std::derived_from<CellType, Cell<K>>; }
	class Rule {
	public:
		using ChunkType = Chunk<K, CellType>;
		using WorldType = World<K, CellType>;

	protected:
		WorldType& world;

	public:

		const K keyhole;

		Rule(WorldType& world, K keyhole) : world(world), keyhole(keyhole) {}

		virtual void update_chunk(ChunkType* working_chunk) = 0;

		void set_cell(ChunkType* working_chunk, Coord target, const CellType& cell) {
			if (working_chunk->in_bound(target))
				working_chunk->set_cell(target, cell);
			else if (world.in_bound(target))
				world.set_cell(target, cell);

			throw std::out_of_range("Coordinate is out of range of the world");
		}

		CellType& get_cell(ChunkType* working_chunk, Coord target) {
			if (working_chunk->in_bound(target))
				return working_chunk->get_cell(target);
			else if (world.in_bound(target))
				return world.get_cell(target);

			throw std::out_of_range("Coordinate is out of range of the world");
		}

		void move_cell(ChunkType* working_chunk, Coord from, Coord to) {
			if (working_chunk->in_bound(from) && working_chunk->in_bound(to))
				return working_chunk->move_cell(working_chunk, from, to);
			else if (world.in_bound(from) && world.in_bound(to))
				return world.move_cell(from, to);

			throw std::out_of_range("Coordinate is out of range of the world");
		}

		void swap_cell(ChunkType* working_chunk, Coord a, Coord b) {
			if (working_chunk->in_bound(a) && working_chunk->in_bound(b))
				return working_chunk->swap_cell(working_chunk, a, b);
			else if (world.in_bound(a) && world.in_bound(b))
				return world.swap_cell(a, b);

			throw std::out_of_range("Coordinate is out of range of the world");
		}
	};

	template <Key K, typename CellType>
	requires requires {std::derived_from < CellType, Cell<K>>; }
	class Chunk {
	public:
		const Coord size;
		const Coord position;

	private:
		using ChunkType = Chunk<K, CellType>;

		std::mt19937 rand_gen;

		CellType* cells;

		std::mutex swapped_mutex;
		// Prevent three-way swap
		bool* swapped;

		std::mutex movements_mutex;
		// source Chunk, index of source Chunk, destination
		std::vector<std::tuple<ChunkType*, size_t, size_t>> movements;

		std::mutex swaps_mutex;
		// Chunk of a, index of a in a Chunk, index of b
		std::vector<std::tuple<ChunkType*, size_t, size_t>> swaps;

		// target index, Cell to replace
		std::vector<std::tuple<size_t, CellType>> reactions;

		// target index
		std::vector<size_t> state_changes;

		std::atomic<size_t> filled_cell_count = 0;

	public:
		Chunk(Coord size, Coord position, std::random_device::result_type rand_result) 
			: size(size), position(position), rand_gen(rand_result) 
		{
			cells = new CellType[size.x * size.y];
			swapped = new bool[size.x * size.y];
		}

		~Chunk() {
			delete[] cells;
		}

		class Iterator {
			CellType* cur;
			size_t diff = 0;
		public:

			Iterator(CellType* cur): cur(cur) {}

			Iterator& operator++() {
				++diff;
				return *this;
			}

			std::pair<CellType&, size_t> operator*() {
				return { cur[diff] , diff};
			}

			bool operator==(const Iterator& ref) {
				return cur + diff == ref.cur + ref.diff;
			}

			bool operator!=(const Iterator& ref) {
				return cur + diff != ref.cur + ref.diff;
			}
		};

		Iterator begin() {
			return Iterator{ cells };
		}

		Iterator end() {
			return Iterator{ cells + size.x * size.y };
		}

		size_t as_index(Coord pos) const {
			return (pos.x - this->position.x) 
				+ (pos.y - this->position.y) * this->size.width;
		}

		Coord as_coord(size_t index) const {
			auto x = index % size.width + position.x;
			auto y = index / size.width + position.y;

			return { x, y };
		}

		bool in_bound(Coord pos) const {
			return pos.x >= this->position.x && pos.x < this->position.x + this->size.width
				&& pos.y >= this->position.y && pos.y < this->position.y + this->size.height;
		}

		bool is_empty(Coord pos) const {
			return is_empty(as_index(pos));
		}

		bool is_empty(size_t index) const {
			return this->cells[index].is_empty();
		}

		bool is_filled() const {
			return filled_cell_count > 0;
		}

		CellType& get_cell(Coord pos) const { return get_cell(as_index(pos)); }
		CellType& get_cell(size_t index) const { return cells[index]; }

		void set_cell(Coord pos, const CellType& cell) { set_cell(as_index(pos), cell); }
		void set_cell(size_t index, const CellType& cell) { 
			auto& target = cells[index];
			if (target.is_empty() && !cell.is_empty()) {
				++filled_cell_count;
			}
			else if(!target.is_empty() && cell.is_empty()) {
				--filled_cell_count;
			}

			target = cell;
		}

		void move_cell(ChunkType* src, Coord from, Coord to) {
			auto lock = std::lock_guard(movements_mutex);
			movements.push_back({ src, src->as_index(from), as_index(to) });
		}

		void swap_cell(ChunkType* src, Coord a, Coord b) {
			auto lock = std::lock_guard(swaps_mutex);
			swaps.push_back({ src, src->as_index(a), as_index(b) });
		}

		void react_cell(Coord target, CellType&& cell) {
			reactions.emplace_back(as_index(target), std::move(cell));
		}

		void change_cell_state(Coord target) {
			state_changes.push_back(as_index(target));
		}

		void commit_movement() {
			constexpr auto dest = [](auto& tup) { return std::get<2>(tup);  };

			for (auto i = 0; i < movements.size(); ++i) {
				if (!is_empty(dest(movements[i]))) {
					movements[i] = movements.back();
					movements.pop_back();
					--i;
				}
			}

			std::sort(movements.begin(), movements.end(), [&](auto& a, auto& b) { return dest(a) < dest(b); });
			
			size_t iprev = 0;
			
			movements.emplace_back(nullptr, SIZE_MAX, SIZE_MAX);
			
			for (size_t i = 0; i < movements.size() - 1; ++i) {
				auto cur = dest(movements[i]);
				auto next = dest(movements[i + 1]);

				if (cur != next) {
					size_t ind = iprev + (rand() % (i - iprev + 1));

					auto [chunk, src, dst] = movements[ind];

					set_cell(dst, chunk->get_cell(src));
					chunk->set_cell(src, CellType());

					iprev = i + 1;
				}
			}
			
			movements.clear();
		}

		void commit_swap() {
			auto b_chunk = this;
			std::shuffle(swaps.begin(), swaps.end(), rand_gen);

			for (auto [a_chunk, a, b] : swaps) {
				auto& a_swapped = a_chunk->swapped[a];
				auto& b_swapped = swapped[b];

				auto in_same_chunk = (a_chunk == b_chunk);

				if (in_same_chunk) { // a chunk == b chunk
					this->swapped_mutex.lock();
				}
				else {
					auto smaller = a_chunk < b_chunk ? a_chunk : b_chunk;
					auto bigger = a_chunk < b_chunk ? b_chunk : a_chunk;

					smaller->swapped_mutex.lock();
					bigger->swapped_mutex.lock();
				}

				if (a_swapped == false && b_swapped == false) { // prevent three-way swap a<->b<->c
					auto temp = a_chunk->cells[a];
					a_chunk->cells[a] = cells[b];
					cells[b] = temp;

					a_chunk->swapped[a] = true;
					swapped[b] = true;
				}

				if (!in_same_chunk) {
					a_chunk->swapped_mutex.unlock();
				}
				b_chunk->swapped_mutex.unlock();
			}

			swaps.clear();

			memset(swapped, 0, size.x * size.y * sizeof(*swapped));
		}

		void commit_reaction() {
			constexpr auto target = [](auto& tup) { return std::get<0>(tup);  };

			std::sort(reactions.begin(), reactions.end(), [&](auto& a, auto& b) { return target(a) < target(b); });

			size_t iprev = 0;

			reactions.emplace_back(SIZE_MAX, CellType());

			for (size_t i = 0; i < reactions.size() - 1; ++i) {
				auto cur = target(reactions[i]);
				auto next = target(reactions[i + 1]);

				if (cur != next) {
					size_t ind = iprev + (rand() % (i - iprev + 1));

					auto [target, cell] = reactions[ind];

					set_cell(target, cell);

					iprev = i + 1;
				}
			}

			reactions.clear();
		}

		void commit_state_change() {
			if (CellType::commit_state_change) {
				for (auto& index : state_changes) {
					cells[index].commit_changes();
				}

				state_changes.clear();
			}
		}
	};

	template <Key K, typename CellType>
		requires requires {std::derived_from<CellType, Cell<K>>; }
	class World {
		using ChunkType = Chunk<K, CellType>;
		using RuleType = Rule<K, CellType>;

		const Coord chunk_size;

		std::random_device rd;

		std::vector<std::unique_ptr<RuleType>> movement_rules;
		std::vector<std::unique_ptr<RuleType>> other_rules;

		concurrency::concurrent_unordered_map< Coord, ChunkType*, Coord::Hasher> chunks;

		std::function<bool(World*, ChunkType*)> is_removable_chunk = 
			[](World* world, ChunkType* chunk) {return !chunk->is_filled(); };



		ChunkType* create_chunk(Coord position) {
			auto chunk = new ChunkType(chunk_size, position, rd());
			chunks.insert({ position, chunk });

			return chunk;
		}

		void remove_useless_chunks() {
			for (auto it = chunks.begin(); it != chunks.end();) {
				if (is_removable_chunk(this, it->second)) {

					delete it->second;
					// because erase is not safe for concurrency, It should run in synchronous context
					it = chunks.unsafe_erase(it); 
				}
				else {
					++it;
				}
			}
		}

	public:
		World(Coord chunk_size): chunk_size(chunk_size) {
			srand(time(NULL));
		}

		void register_rule(std::unique_ptr<RuleType> rule, bool is_movement_rule) {
			if (is_movement_rule) {
				this->movement_rules.emplace_back(std::move(rule));
			}
			else {
				this->other_rules.emplace_back(std::move(rule));
			}
		}

		Coord get_chunk_position(Coord cell_position) const {
			return { cell_position.x - cell_position.x % chunk_size.width, 
				cell_position.y - cell_position.y % chunk_size.height };
		}

		ChunkType* get_chunk_direct(Coord position) const {
			auto iter = chunks.find(position);
			auto end = chunks.end();

			return iter != end ? iter->second : nullptr;
		}

		ChunkType* get_chunk(Coord position) {
			auto pos = get_chunk_position(position);
			auto chunk = get_chunk_direct(pos);

			return chunk != nullptr ? chunk : create_chunk(pos);
		}

		bool check_chunk_exist(Coord position) {
			auto pos = get_chunk_position(position);
			return chunks.contains(pos);
		}

		bool in_bound(Coord position) {
			if (auto chunk = get_chunk(position)) {
				return chunk->in_bound(position);
			}

			return false;
		}

		bool is_empty(Coord position) {
			return in_bound(position) && get_chunk(position)->is_empty(position);
		}

		CellType& get_cell(Coord position) {
			return get_chunk(position)->get_cell(position);
		}

		void set_cell(Coord position, const CellType& cell) {
			if (auto chunk = get_chunk(position)) {
				chunk->set_cell(position, cell);
			}
		}

		void move_cell(Coord from, Coord to) {
			if(auto src = get_chunk(from))
				if (auto dest = get_chunk(to)) {
					dest->move_cell(src, from, to);
			}
		}

		void swap_cell(Coord a, Coord b) {
			if(auto a_chunk = get_chunk(a))
				if (auto b_chunk = get_chunk(b)) {
					b_chunk->swap_cell(a_chunk, a, b);
				}
		}

		void tick() {
			remove_useless_chunks();

			//Process other rules first then process movement or swap rules
			//Running all rules simultaneously can cause improper action

			for (auto& rule : other_rules) {
				for (auto& [_, chunk] : chunks) {
					rule->update_chunk(chunk);
				}
			}

			for (auto& [_, chunk] : chunks) {
				chunk->commit_reaction();
				chunk->commit_state_change();
			}


			for (auto& rule : movement_rules) {
				for (auto& [_, chunk] : chunks) {
					rule->update_chunk(chunk);
				}
			}

			for (auto& [_, chunk] : chunks) {
				chunk->commit_movement();
				chunk->commit_swap();
			}
		}
	};
}