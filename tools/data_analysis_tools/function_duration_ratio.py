import pandas as pd
import numpy as np
import os

# ================= 用户配置 =================

INPUT_FILE = "daft_performance_full_report_0312_0.7.5_v2.xlsx"
SHEET_NAME = "tpch"
OUTPUT_REPORT = 'daft_impact_analysis_updated_0312_0.7.5_v2.xlsx'

OPTIMIZATION_GROUPS = [
    {"精确匹配": ["alloc::boxed::iter::<impl core::iter::traits::iterator::Iterator for alloc::boxed::Box<I,A>>::next"], "模糊匹配": []},
    {"精确匹配": ["snap::decompress::Decoder::decompress"], "模糊匹配": []},
    {"精确匹配": ["arrow_select::take::take_bytes"], "模糊匹配": []},
    {"精确匹配": ["<alloc::vec::Vec<T> as alloc::vec::spec_from_iter::SpecFromIter<T,I>>::from_iter"], "模糊匹配": []},
    {"精确匹配": ["daft_core::kernels::hashing::hash_primitive"], "模糊匹配": []},
    {"精确匹配": ["arrow_ord::ord::compare_impl::{{closure}}"], "模糊匹配": []},
    {"精确匹配": ["<daft_core::array::growable::arrow_growable::ArrowGrowable<T> as daft_core::array::growable::Growable>::extend"], "模糊匹配": []},
    {"精确匹配": ["daft_recordbatch::ops::hash::<impl daft_recordbatch::RecordBatch>::to_idx_hash_table"], "模糊匹配": []},
    {"精确匹配": ["daft_core::kernels::hashing::hash_large_binary"], "模糊匹配": []},
    {"精确匹配": ["hashbrown::raw::RawTable<T,A>::reserve_rehash"], "模糊匹配": []},
    {"精确匹配": ["core::ops::function::Fn::call"], "模糊匹配": []},
    {"精确匹配": ["daft_recordbatch::ops::hash::<impl daft_recordbatch::RecordBatch>::to_probe_hash_table"], "模糊匹配": []},
    {"精确匹配": ["hashbrown::map::HashMap<K,V,S,A>::insert"], "模糊匹配": []},
    {"精确匹配": ["daft_core::array::ops::arrow::comparison::build_multi_array_is_equal_from_arrays::{{closure}}"], "模糊匹配": []},
    {"精确匹配": ["parquet::arrow::buffer::offset_buffer::OffsetBuffer<I>::extend_from_dictionary"], "模糊匹配": []},
    {"精确匹配": ["parquet::encodings::rle::RleDecoder::get_batch_with_dict"], "模糊匹配": []},
    {"精确匹配": ["arrow_select::filter::FilterBytes<OffsetSize>::extend_offsets_slices"], "模糊匹配": []},
    {"精确匹配": ["daft_recordbatch::ops::groups::<impl daft_core::array::ops::IntoUniqueIdxs for daft_recordbatch::RecordBatch>::make_unique_idxs"], "模糊匹配": []},
    {"精确匹配": ["<daft_recordbatch::probeable::probe_table::ProbeTableBuilder as daft_recordbatch::probeable::ProbeableBuilder>::add_table"], "模糊匹配": []},
    {"精确匹配": ["<core::iter::adapters::map::Map<I,F> as core::iter::traits::iterator::Iterator>::try_fold"], "模糊匹配": []},
    {"精确匹配": ["<daft_core::array::growable::arrow_growable::ArrowGrowable<T> as daft_core::array::growable::Growable>::add_nulls"], "模糊匹配": []},
    {"精确匹配": ["arrow_select::concat::concat_bytes"], "模糊匹配": []},
    {"精确匹配": ["daft_core::array::ops::arrow::comparison::build_is_equal::{{closure}}"], "模糊匹配": []},
    {"精确匹配": ["<alloc::vec::Vec<T,A> as alloc::vec::spec_extend::SpecExtend<T,I>>::spec_extend"], "模糊匹配": []},
    {"精确匹配": ["alloc::sync::Arc<T,A>::drop_slow"], "模糊匹配": []},
    {"精确匹配": ["core::ptr::drop_in_place<arrow_data::data::ArrayData>"], "模糊匹配": []},
    {"精确匹配": ["daft_recordbatch::growable::GrowableRecordBatch::add_nulls"], "模糊匹配": []},
    {"精确匹配": ["parquet::arrow::arrow_reader::selection::RowSelectionCursor::new_mask_from_selectors"], "模糊匹配": []},
    {"精确匹配": ["arrow_array::array::boolean_array::BooleanArray::from_trusted_len_iter"], "模糊匹配": []},
    {"精确匹配": ["<core::iter::adapters::cloned::Cloned<I> as core::iter::traits::iterator::Iterator>::fold"], "模糊匹配": []},
    {"精确匹配": ["<core::iter::adapters::map::Map<I,F> as core::iter::traits::iterator::Iterator>::fold"], "模糊匹配": []},
    {"精确匹配": ["parquet::arrow::arrow_reader::selection::RowSelection::from_filters"], "模糊匹配": []},
    {"精确匹配": ["<arrow_array::array::primitive_array::PrimitiveArray<T> as arrow_array::array::Array>::to_data"], "模糊匹配": []},
    {"精确匹配": ["daft_core::array::ops::take::<impl daft_core::array::DataArray<T>>::take"], "模糊匹配": []},
    {"精确匹配": ["core::ptr::drop_in_place<alloc::vec::Vec<arrow_buffer::buffer::immutable::Buffer>>"], "模糊匹配": []},
    {"精确匹配": ["daft_core::array::ops::concat::<impl daft_core::array::DataArray<T>>::concat"], "模糊匹配": []},
    {"精确匹配": ["<arrow_array::array::byte_array::GenericByteArray<T> as arrow_array::array::Array>::to_data"], "模糊匹配": []},
    {"精确匹配": ["core::ptr::drop_in_place<arrow_schema::datatype::DataType>"], "模糊匹配": []},
    {"精确匹配": ["daft_core::series::ops::concat::<impl daft_core::series::Series>::concat"], "模糊匹配": []},
    {"精确匹配": ["<parquet::arrow::arrow_reader::ParquetRecordBatchReader as core::iter::traits::iterator::Iterator>::next"], "模糊匹配": []},
    {"精确匹配": ["core::ptr::drop_in_place<arrow_array::array::primitive_array::PrimitiveArray<arrow_array::types::Int8Type>>"], "模糊匹配": []},
    {"精确匹配": ["<core::iter::adapters::map::Map<I,F> as core::iter::traits::iterator::Iterator>::next"], "模糊匹配": []},
    {"精确匹配": ["<arrow_schema::datatype::DataType as core::cmp::PartialEq>::eq"], "模糊匹配": []},
    {"精确匹配": ["parquet::util::bit_util::BitReader::get_value"], "模糊匹配": []},
    {"精确匹配": ["__rustc::__rust_alloc"], "模糊匹配": []},
    {"精确匹配": ["daft_core::array::ops::arrow::comparison::build_is_equal_with_nan::{{closure}}"], "模糊匹配": []},
    {"精确匹配": ["daft_core::series::array_impl::data_array::<impl daft_core::series::series_like::SeriesLike for daft_core::series::array_impl::ArrayWrapper<daft_core::array::DataArray<daft_core::datatypes::Int64Type>>>::take"], "模糊匹配": []},
    {"精确匹配": ["<alloc::vec::Vec<T,A> as core::ops::drop::Drop>::drop"], "模糊匹配": []},
    {"精确匹配": ["<daft_schema::field::Field as core::clone::Clone>::clone"], "模糊匹配": []},
    {"精确匹配": ["daft_dsl::expr::Expr::to_field"], "模糊匹配": []},
    {"精确匹配": ["<core::iter::adapters::chain::Chain<A,B> as core::iter::traits::iterator::Iterator>::fold"], "模糊匹配": []},
    {"精确匹配": ["<tracing::instrument::Instrumented<T> as core::future::future::Future>::poll"], "模糊匹配": []},
    {"精确匹配": ["daft_core::series::ops::downcast::<impl daft_core::series::Series>::downcast"], "模糊匹配": []},
    {"精确匹配": ["<tokio::util::idle_notified_set::ListEntry<T> as tokio::util::wake::Wake>::wake_by_ref"], "模糊匹配": []},
    {"精确匹配": ["daft_core::array::ops::arrow::comparison::build_multi_array_is_equal_from_arrays"], "模糊匹配": []},
    {"精确匹配": ["core::slice::sort::unstable::quicksort::quicksort"], "模糊匹配": []},
    {"精确匹配": ["parquet::arrow::buffer::offset_buffer::OffsetBuffer<I>::try_push"], "模糊匹配": []},
    {"精确匹配": ["alloc::raw_vec::RawVec<T,A>::grow_one"], "模糊匹配": []},
    {"精确匹配": ["parquet::arrow::array_reader::byte_array::ByteArrayDecoderPlain::read"], "模糊匹配": []},
    {"精确匹配": ["daft_core::array::ops::count::grouped_count_arrow_bitmap"], "模糊匹配": []},
    {"精确匹配": ["core::slice::sort::shared::pivot::median3_rec"], "模糊匹配": []},
    {"精确匹配": ["arrow_ord::ord::make_comparator"], "模糊匹配": []},
    {"精确匹配": ["arrow_data::data::ArrayDataBuilder::build"], "模糊匹配": []},
    {"精确匹配": ["arrow_data::data::ArrayDataBuilder::build_unchecked"], "模糊匹配": []},
    {"精确匹配": ["core::ptr::drop_in_place<arrow_ord::ord::compare_impl<_,_,arrow_ord::ord::compare_primitive<arrow_array::types::Int8Type>::{{closure}}>::{{closure}}>"], "模糊匹配": []},
    {"精确匹配": ["arrow_data::data::ArrayDataBuilder::nulls"], "模糊匹配": []},
    {"精确匹配": ["arrow_array::array::primitive_array::<impl core::convert::From<arrow_array::array::primitive_array::PrimitiveArray<T>> for arrow_data::data::ArrayData>::from"], "模糊匹配": []},
    {"精确匹配": ["daft_recordbatch::growable::GrowableRecordBatch::new"], "模糊匹配": []},
    {"精确匹配": ["arrow_data::data::ArrayDataBuilder::buffers"], "模糊匹配": []},
    {"精确匹配": ["daft_core::array::ops::arrow::comparison::build_is_equal"], "模糊匹配": []},
    {"精确匹配": ["<daft_schema::field::Field as core::cmp::PartialEq>::eq"], "模糊匹配": []},
    {"精确匹配": ["core::ptr::drop_in_place<daft_core::array::ops::arrow::comparison::build_is_equal::{{closure}}>"], "模糊匹配": []},
    {"精确匹配": ["parquet::util::bit_util::BitReader::get_batch"], "模糊匹配": []},
    {"精确匹配": ["chrono::naive::date::NaiveDate::from_num_days_from_ce_opt"], "模糊匹配": []},
    {"精确匹配": ["parquet::util::bit_pack::unpack32::unpack"], "模糊匹配": []},
    {"精确匹配": ["<arrow_schema::datatype::DataType as core::clone::Clone>::clone"], "模糊匹配": []},
    {"精确匹配": ["parquet::util::bit_pack::unpack32"], "模糊匹配": []},
    {"精确匹配": ["parquet::file::metadata::thrift::PageHeader::read_thrift_without_stats"], "模糊匹配": []},
    {"精确匹配": ["std::io::default_read_to_end"], "模糊匹配": []},
    {"精确匹配": ["<daft_schema::dtype::DataType as core::clone::Clone>::clone"], "模糊匹配": []},
    {"精确匹配": ["daft_schema::dtype::DataType::to_arrow"], "模糊匹配": []},
    {"精确匹配": ["parquet::file::serialized_reader::decode_page"], "模糊匹配": []},
    {"精确匹配": ["__rustc::__rust_dealloc"], "模糊匹配": []},
    {"精确匹配": ["<alloc::sync::Arc<dyn arrow_array::array::Array> as arrow_array::array::Array>::len"], "模糊匹配": []},
    {"精确匹配": ["daft_recordbatch::ops::agg::<impl daft_recordbatch::RecordBatch>::agg_groupby"], "模糊匹配": []},
    {"精确匹配": ["daft_core::array::DataArray<T>::from_arrow"], "模糊匹配": []},
    {"精确匹配": ["tokio::sync::task::atomic_waker::AtomicWaker::wake"], "模糊匹配": []},
    {"精确匹配": ["tokio::runtime::task::harness::Harness<T,S>::complete"], "模糊匹配": []},
    {"精确匹配": ["tokio::util::sharded_list::ShardedList<L,<L as tokio::util::linked_list::Link>::Target>::remove"], "模糊匹配": []},
    {"精确匹配": ["<parquet::compression::snappy_codec::SnappyCodec as parquet::compression::Codec>::decompress"], "模糊匹配": []},
    {"精确匹配": ["<snafu::futures::try_future::WithContext<Fut,F,E> as core::future::future::Future>::poll"], "模糊匹配": []},
    {"精确匹配": ["mio::poll::Poll::poll"], "模糊匹配": []},
    {"精确匹配": ["<parquet::arrow::array_reader::primitive_array::PrimitiveArrayReader<T> as parquet::arrow::array_reader::ArrayReader>::consume_batch"], "模糊匹配": []},
    {"精确匹配": ["<arrow_buffer::util::bit_iterator::BitSliceIterator as core::iter::traits::iterator::Iterator>::next"], "模糊匹配": []},
    {"精确匹配": ["<core::hash::sip::Hasher<S> as core::hash::Hasher>::write"], "模糊匹配": []},
    {"精确匹配": ["daft_recordbatch::RecordBatch::eval_expression_list"], "模糊匹配": []},
    {"精确匹配": ["daft_core::array::from::<impl daft_core::array::DataArray<T>>::from_field_and_values"], "模糊匹配": []},
    {"精确匹配": ["tokio::runtime::scheduler::multi_thread::handle::Handle::bind_new_task"], "模糊匹配": []},
    {"精确匹配": ["core::iter::adapters::try_process"], "模糊匹配": []},
    {"精确匹配": ["core::ptr::drop_in_place<arrow_array::array::primitive_array::PrimitiveArray<arrow_array::types::IntervalMonthDayNanoType>>"], "模糊匹配": []},
    {"精确匹配": ["bytes::bytes::shared_drop"], "模糊匹配": []},
    {"精确匹配": ["core::ptr::drop_in_place<daft_schema::field::Field>"], "模糊匹配": []},
    {"精确匹配": ["tokio::runtime::task::list::OwnedTasks<S>::bind_inner"], "模糊匹配": []},
    {"精确匹配": ["arrow_select::take::take_impl"], "模糊匹配": []},
    {"精确匹配": ["<tokio::util::idle_notified_set::IdleNotifiedSet<T> as core::ops::drop::Drop>::drop"], "模糊匹配": []},
    {"精确匹配": ["tracing::instrument::_::<impl core::ops::drop::Drop for tracing::instrument::Instrumented<T>>::drop"], "模糊匹配": []},
    {"精确匹配": ["tokio::util::metric_atomics::MetricAtomicU64::add"], "模糊匹配": []},
    {"精确匹配": ["arrow_select::filter::filter_native"], "模糊匹配": []},
    {"精确匹配": ["parquet::util::bit_util::BitReader::get_vlq_int"], "模糊匹配": []},
    {"精确匹配": ["bytes::bytes::shared_clone"], "模糊匹配": []},
    {"精确匹配": ["bytes::bytes::promotable_even_clone"], "模糊匹配": []},
    {"精确匹配": ["bytes::bytes::promotable_even_drop"], "模糊匹配": []},
    {"精确匹配": ["alloc::raw_vec::finish_grow"], "模糊匹配": []},
    {"精确匹配": ["tokio::util::idle_notified_set::IdleNotifiedSet<T>::pop_notified"], "模糊匹配": []},
    {"精确匹配": ["tokio::runtime::scheduler::current_thread::Core::next_task"], "模糊匹配": []},
    {"精确匹配": ["<daft_core::array::growable::arrow_growable::ArrowGrowable<T> as daft_core::array::growable::Growable>::build"], "模糊匹配": []},
    {"精确匹配": ["daft_core::series::array_impl::data_array::<impl daft_core::series::series_like::SeriesLike for daft_core::series::array_impl::ArrayWrapper<daft_core::array::DataArray<daft_core::datatypes::FixedSizeBinaryType>>>::len"], "模糊匹配": []},
    {"精确匹配": ["arrow_ord::cmp::apply_op"], "模糊匹配": []},
    {"精确匹配": ["parquet::column::reader::GenericColumnReader<R,D,V>::skip_records"], "模糊匹配": []},
    {"精确匹配": ["parquet::column::reader::GenericColumnReader<R,D,V>::read_records"], "模糊匹配": []},
    {"精确匹配": ["<parquet::arrow::record_reader::definition_levels::DefinitionLevelBufferDecoder as parquet::column::reader::decoder::DefinitionLevelDecoder>::read_def_levels"], "模糊匹配": []},
    {"精确匹配": ["core::hash::BuildHasher::hash_one"], "模糊匹配": []},
    {"精确匹配": ["<parquet::arrow::array_reader::primitive_array::PrimitiveArrayReader<T> as parquet::arrow::array_reader::ArrayReader>::read_records"], "模糊匹配": []},
    {"精确匹配": ["<parquet::column::reader::decoder::ColumnValueDecoderImpl<T> as parquet::column::reader::decoder::ColumnValueDecoder>::read"], "模糊匹配": []},
    {"精确匹配": ["parquet::encodings::rle::RleDecoder::skip"], "模糊匹配": []},
    {"精确匹配": ["parquet::util::bit_util::BitReader::skip"], "模糊匹配": []},
    {"精确匹配": ["<parquet::arrow::record_reader::definition_levels::DefinitionLevelBufferDecoder as parquet::column::reader::decoder::DefinitionLevelDecoder>::skip_def_levels"], "模糊匹配": []},
    {"精确匹配": ["<parquet::file::serialized_reader::SerializedPageReader<R> as parquet::column::page::PageReader>::get_next_page"], "模糊匹配": []},
    {"精确匹配": ["parquet::encodings::rle::RleDecoder::get_batch"], "模糊匹配": []},
    {"精确匹配": ["<parquet::arrow::array_reader::struct_array::StructArrayReader as parquet::arrow::array_reader::ArrayReader>::read_records"], "模糊匹配": []},
    {"精确匹配": ["<parquet::arrow::array_reader::byte_array::ByteArrayReader<I> as parquet::arrow::array_reader::ArrayReader>::read_records"], "模糊匹配": []},
    {"精确匹配": ["daft_core::array::ops::cast::<impl daft_core::array::DataArray<T>>::cast"], "模糊匹配": []},
    {"精确匹配": ["daft_recordbatch::RecordBatch::eval_expression_internal"], "模糊匹配": []},
    {"精确匹配": ["bytes::bytes::Bytes::slice"], "模糊匹配": []},
    {"精确匹配": ["<parquet::arrow::array_reader::struct_array::StructArrayReader as parquet::arrow::array_reader::ArrayReader>::skip_records"], "模糊匹配": []},
    {"精确匹配": ["parquet::encodings::rle::RleDecoder::reload"], "模糊匹配": []},
    {"精确匹配": ["daft_core::array::ops::groups::make_groups"], "模糊匹配": []},
    {"精确匹配": ["core::ops::function::impls::<impl core::ops::function::FnOnce<A> for &mut F>::call_once"], "模糊匹配": []},
    {"精确匹配": ["parquet::util::bit_pack::unpack16::unpack"], "模糊匹配": []},
    {"精确匹配": ["parquet::column::reader::GenericColumnReader<R,D,V>::read_new_page"], "模糊匹配": []},
    {"精确匹配": ["daft_core::array::ops::if_else::generic_if_else"], "模糊匹配": []},
    {"精确匹配": ["parquet::arrow::array_reader::byte_array::ByteArrayDecoder::skip"], "模糊匹配": []},
    {"精确匹配": ["<parquet::encodings::decoding::PlainDecoder<T> as parquet::encodings::decoding::Decoder<T>>::get"], "模糊匹配": []},
    {"精确匹配": ["arrow_buffer::buffer::ops::buffer_bin_and"], "模糊匹配": []},
    {"精确匹配": ["<parquet::encodings::decoding::DictDecoder<T> as parquet::encodings::decoding::Decoder<T>>::get"], "模糊匹配": []},
    {"精确匹配": ["daft_core::series::from_lit::series_from_literals_iter"], "模糊匹配": []},
    {"精确匹配": ["<parquet::encodings::decoding::PlainDecoder<T> as parquet::encodings::decoding::Decoder<T>>::skip"], "模糊匹配": []},
    {"精确匹配": ["parquet::file::page_index::offset_index::OffsetIndexMetaData::try_from_fast"], "模糊匹配": []},
    {"精确匹配": ["<parquet::encodings::decoding::DictDecoder<T> as parquet::encodings::decoding::Decoder<T>>::skip"], "模糊匹配": []},
    {"精确匹配": ["<daft_schema::dtype::DataType as core::cmp::PartialEq>::eq"], "模糊匹配": []},
    {"精确匹配": ["<alloc::vec::into_iter::IntoIter<T,A> as core::iter::traits::iterator::Iterator>::try_fold"], "模糊匹配": []},
    {"精确匹配": ["parquet::util::bit_pack::unpack8"], "模糊匹配": []},
    {"精确匹配": ["alloc::raw_vec::RawVecInner<A>::reserve::do_reserve_and_handle"], "模糊匹配": []},
    {"精确匹配": ["daft_dsl::treenode::with_new_children_if_necessary"], "模糊匹配": []},
    {"精确匹配": ["daft_core::series::ops::comparison::<impl daft_core::array::ops::DaftCompare<&daft_core::series::Series> for daft_core::series::Series>::equal"], "模糊匹配": []},
    {"精确匹配": ["arrow_buffer::buffer::scalar::ScalarBuffer<T>::new"], "模糊匹配": []},
    {"精确匹配": ["arrow_buffer::buffer::mutable::MutableBuffer::reallocate"], "模糊匹配": []},
    {"精确匹配": ["core::ptr::drop_in_place<daft_core::series::array_impl::ArrayWrapper<daft_core::array::DataArray<daft_core::datatypes::Int8Type>>>"], "模糊匹配": []},
    {"精确匹配": ["<parquet::encodings::decoding::DictDecoder<T> as parquet::encodings::decoding::Decoder<T>>::set_data"], "模糊匹配": []},
    {"精确匹配": ["alloc::alloc::exchange_malloc"], "模糊匹配": []},
    {"精确匹配": ["daft_schema::schema::Schema::get_index"], "模糊匹配": []},
    {"精确匹配": ["daft_dsl::expr::Expr::children"], "模糊匹配": []},
    {"精确匹配": ["parquet::arrow::record_reader::definition_levels::PackedDecoder::next_rle_block"], "模糊匹配": []},
    {"精确匹配": ["std::os::unix::net::datagram::UnixDatagram::try_clone"], "模糊匹配": []},
    {"精确匹配": ["core::ptr::drop_in_place<arrow_array::array::byte_array::GenericByteArray<arrow_array::types::GenericBinaryType<i32>>>"], "模糊匹配": []},
    {"精确匹配": ["arrow_cast::cast::cast_with_options"], "模糊匹配": []},
    {"精确匹配": ["daft_local_execution::sinks::grouped_aggregate::AggStrategy::execute_strategy"], "模糊匹配": []},
    {"精确匹配": ["arrow_array::array::primitive_array::PrimitiveArray<T>::try_new"], "模糊匹配": []},
    {"精确匹配": ["<futures_util::future::try_join_all::TryJoinAll<F> as core::future::future::Future>::poll"], "模糊匹配": []},
    {"精确匹配": ["arrow_select::filter::FilterBytes<OffsetSize>::extend_idx"], "模糊匹配": []},
    {"精确匹配": ["arrow_buffer::util::bit_chunk_iterator::UnalignedBitChunk::count_ones"], "模糊匹配": []},
    {"精确匹配": ["parquet::parquet_thrift::ThriftCompactInputProtocol::skip_till_depth"], "模糊匹配": []},
    {"精确匹配": ["bytes::bytes::shallow_clone_vec"], "模糊匹配": []},
    {"精确匹配": ["<arrow_array::array::primitive_array::PrimitiveArray<T> as core::convert::From<arrow_data::data::ArrayData>>::from"], "模糊匹配": []},
    {"精确匹配": ["arrow_buffer::buffer::boolean::BooleanBuffer::count_set_bits"], "模糊匹配": []},
    {"精确匹配": ["arrow_buffer::buffer::boolean::BooleanBuffer::from_bitwise_unary_op"], "模糊匹配": []},
    {"精确匹配": ["arrow_select::filter::FilterBytes<OffsetSize>::extend_slices"], "模糊匹配": []},
    {"精确匹配": ["<parquet::arrow::arrow_reader::ReaderPageIterator<T> as core::iter::traits::iterator::Iterator>::next"], "模糊匹配": []},
    {"精确匹配": ["<parquet::basic::PageType as parquet::parquet_thrift::ReadThrift<R>>::read_thrift"], "模糊匹配": []},
    {"精确匹配": ["parking_lot::condvar::Condvar::wait_until_internal"], "模糊匹配": []},
    {"精确匹配": ["core::ptr::drop_in_place<daft_schema::dtype::DataType>"], "模糊匹配": []},
    {"精确匹配": ["daft_core::series::ops::hash::<impl daft_core::series::Series>::hash"], "模糊匹配": []},
    {"精确匹配": ["parquet::parquet_thrift::read_thrift_vec"], "模糊匹配": []},
    {"精确匹配": ["<daft_schema::field::Field as core::convert::TryFrom<&arrow_schema::field::Field>>::try_from"], "模糊匹配": []},
    {"精确匹配": ["<std::fs::File as parquet::file::reader::ChunkReader>::get_bytes"], "模糊匹配": []},
    {"精确匹配": ["daft_dsl::expr::Expr::with_new_children"], "模糊匹配": []},
    {"精确匹配": ["daft_core::series::ops::logical::<impl daft_core::array::ops::DaftLogical<&daft_core::series::Series> for daft_core::series::Series>::and"], "模糊匹配": []},
    {"精确匹配": ["daft_parquet::arrowrs_reader::finalize_batch"], "模糊匹配": []},
    {"精确匹配": ["tokio::runtime::scheduler::multi_thread::queue::Steal<T>::steal_into"], "模糊匹配": []},
    {"精确匹配": ["tokio::runtime::scheduler::multi_thread::worker::Context::run"], "模糊匹配": []},
    {"精确匹配": ["arrow_select::filter::filter_primitive"], "模糊匹配": []},
    {"精确匹配": ["core::ptr::drop_in_place<daft_core::array::DataArray<daft_core::datatypes::Utf8Type>>"], "模糊匹配": []},
    {"精确匹配": ["daft_recordbatch::ops::groups::<impl daft_core::array::ops::IntoGroups for daft_recordbatch::RecordBatch>::make_groups"], "模糊匹配": []},
    {"精确匹配": ["__rustc::__rust_no_alloc_shim_is_unstable_v2"], "模糊匹配": []},
    {"精确匹配": ["<alloc::sync::Arc<dyn arrow_array::array::Array> as arrow_array::array::Array>::as_any"], "模糊匹配": []},
    {"精确匹配": ["daft_core::array::ops::groups::make_unique_idxs"], "模糊匹配": []},
    {"精确匹配": ["core::slice::sort::shared::smallsort::small_sort_network"], "模糊匹配": []},
    {"精确匹配": ["<futures_util::future::try_future::into_future::IntoFuture<Fut> as core::future::future::Future>::poll"], "模糊匹配": []},
    {"精确匹配": ["arrow_buffer::util::bit_chunk_iterator::UnalignedBitChunk::new"], "模糊匹配": []},
    {"精确匹配": ["daft_local_execution::intermediate_ops::intermediate_op::IntermediateNode<Op>::handle_task_completion::{{closure}}"], "模糊匹配": []},
    {"精确匹配": ["core::ptr::drop_in_place<daft_core::array::ops::arrow::comparison::build_is_equal_with_nan::{{closure}}>"], "模糊匹配": []},
    {"精确匹配": ["arrow_select::take::take_native"], "模糊匹配": []},
    {"精确匹配": ["core::ptr::drop_in_place<daft_micropartition::micropartition::MicroPartition>"], "模糊匹配": []},
    {"精确匹配": ["<parquet::basic::Encoding as parquet::parquet_thrift::ReadThrift<R>>::read_thrift"], "模糊匹配": []},
    {"精确匹配": ["daft_core::array::ops::get::<impl daft_core::array::DataArray<daft_core::datatypes::Utf8Type>>::get"], "模糊匹配": []},
    {"精确匹配": ["arrow_array::builder::generic_bytes_builder::GenericByteBuilder<T>::append_value"], "模糊匹配": []},
    {"精确匹配": ["daft_functions_utf8::left::left_impl::left_most_chars"], "模糊匹配": []},
    {"精确匹配": ["<arrow_array::array::byte_array::GenericByteArray<T> as core::iter::traits::collect::FromIterator<core::option::Option<Ptr>>>::from_iter"], "模糊匹配": []},
    {"精确匹配": ["<arrow_select::filter::IndexIterator as core::iter::traits::iterator::Iterator>::next"], "模糊匹配": []},
    {"精确匹配": ["parquet::arrow::arrow_reader::read_plan::ReadPlanBuilder::build"], "模糊匹配": []},
    {"精确匹配": ["tokio::util::sharded_list::ShardGuard<L,<L as tokio::util::linked_list::Link>::Target>::push"], "模糊匹配": []},
    {"精确匹配": ["daft_core::array::ops::apply::<impl daft_core::array::DataArray<T>>::apply"], "模糊匹配": []},
    {"精确匹配": ["<arrow_array::array::primitive_array::PrimitiveArray<T> as arrow_array::array::Array>::len"], "模糊匹配": []},
    {"精确匹配": ["parquet::util::bit_pack::unpack16"], "模糊匹配": []},
    {"精确匹配": ["arrow_array::array::make_array"], "模糊匹配": []},
    {"精确匹配": ["std::io::copy::stack_buffer_copy"], "模糊匹配": []},
    {"精确匹配": ["<arrow_array::array::primitive_array::PrimitiveArray<T> as arrow_array::array::Array>::nulls"], "模糊匹配": []},
    {"精确匹配": ["<hashbrown::raw::RawTable<T,A> as core::ops::drop::Drop>::drop"], "模糊匹配": []},
    {"精确匹配": ["parquet::column::reader::parse_v1_level"], "模糊匹配": []},
    {"精确匹配": ["<parquet::arrow::record_reader::definition_levels::DefinitionLevelBufferDecoder as parquet::column::reader::decoder::ColumnLevelDecoder>::set_data"], "模糊匹配": []},
    {"精确匹配": ["daft_schema::dtype::DataType::to_physical"], "模糊匹配": []},
    {"精确匹配": ["parquet::file::statistics::from_thrift_page_stats"], "模糊匹配": []},
    {"精确匹配": ["tokio::runtime::task::harness::Harness<T,S>::poll"], "模糊匹配": []},
    {"精确匹配": ["daft_core::series::ops::comparison::<impl daft_core::array::ops::DaftCompare<&daft_core::series::Series> for daft_core::series::Series>::gte"], "模糊匹配": []},
    {"精确匹配": ["tokio::runtime::scheduler::multi_thread::worker::block_in_place"], "模糊匹配": []},
    {"精确匹配": ["core::ptr::drop_in_place<daft_core::array::DataArray<daft_core::datatypes::Int32Type>>"], "模糊匹配": []},
    {"精确匹配": ["<parquet::encodings::decoding::PlainDecoder<T> as parquet::encodings::decoding::Decoder<T>>::set_data"], "模糊匹配": []},
    {"精确匹配": ["arrow_array::array::primitive_array::PrimitiveArray<T>::reinterpret_cast"], "模糊匹配": []},
    {"精确匹配": ["alloc::vec::in_place_collect::<impl alloc::vec::spec_from_iter::SpecFromIter<T,I> for alloc::vec::Vec<T>>::from_iter"], "模糊匹配": []},
    {"精确匹配": ["daft_recordbatch::RecordBatch::from_arrow"], "模糊匹配": []},
    {"精确匹配": ["<parquet::data_type::ByteArray as core::convert::From<&[u8]>>::from"], "模糊匹配": []},
    {"精确匹配": ["<tokio::runtime::task::join::JoinHandle<T> as core::future::future::Future>::poll"], "模糊匹配": []},
    {"精确匹配": ["daft_recordbatch::RecordBatch::cast_to_schema_with_fill"], "模糊匹配": []},
    {"精确匹配": ["arrow_buffer::builder::null::NullBufferBuilder::finish"], "模糊匹配": []},
    {"精确匹配": ["arrow_cast::cast::can_cast_types"], "模糊匹配": []},
    {"精确匹配": ["parking_lot::condvar::Condvar::notify_one_slow"], "模糊匹配": []},
    {"精确匹配": ["core::ptr::drop_in_place<[daft_schema::field::Field]>"], "模糊匹配": []},
    {"精确匹配": ["core::fmt::Formatter::pad"], "模糊匹配": []},
    {"精确匹配": ["<daft_local_execution::ExecutionTaskSpawner as core::clone::Clone>::clone"], "模糊匹配": []},
    {"精确匹配": ["<daft_recordbatch::RecordBatch as core::convert::TryFrom<&arrow_array::record_batch::RecordBatch>>::try_from"], "模糊匹配": []},
    {"精确匹配": ["daft_core::series::array_impl::data_array::<impl daft_core::series::series_like::SeriesLike for daft_core::series::array_impl::ArrayWrapper<daft_core::array::DataArray<daft_core::datatypes::Int8Type>>>::data_type"], "模糊匹配": []},
    {"精确匹配": ["<core::iter::adapters::cloned::Cloned<I> as core::iter::traits::iterator::Iterator>::next"], "模糊匹配": []},
    {"精确匹配": ["daft_core::series::Series::from_arrow"], "模糊匹配": []},
    {"精确匹配": ["<alloc::vec::into_iter::IntoIter<T,A> as core::ops::drop::Drop>::drop"], "模糊匹配": []},
    {"精确匹配": ["arrow_buffer::buffer::null::NullBuffer::new"], "模糊匹配": []},
    {"精确匹配": ["<parquet::arrow::array_reader::struct_array::StructArrayReader as parquet::arrow::array_reader::ArrayReader>::consume_batch"], "模糊匹配": []},
    {"精确匹配": ["<parquet::arrow::arrow_reader::ReaderRowGroups<T> as parquet::arrow::array_reader::RowGroups>::column_chunks"], "模糊匹配": []},
    {"精确匹配": ["core::ptr::drop_in_place<arrow_array::record_batch::RecordBatch>"], "模糊匹配": []},
    {"精确匹配": ["core::ptr::drop_in_place<daft_core::series::array_impl::ArrayWrapper<daft_core::datatypes::logical::LogicalArrayImpl<daft_core::datatypes::DateType,daft_core::array::DataArray<daft_core::datatypes::Int32Type>>>>"], "模糊匹配": []},
    {"精确匹配": ["daft_core::array::ops::comparison::<impl daft_core::array::ops::DaftCompare<&daft_core::array::DataArray<T>> for daft_core::array::DataArray<T>>::lt"], "模糊匹配": []},
    {"精确匹配": ["daft_core::datatypes::logical::LogicalArrayImpl<L,P>::new"], "模糊匹配": []},
    {"精确匹配": ["parking_lot_core::parking_lot::lock_bucket_pair"], "模糊匹配": []},
    {"精确匹配": ["parquet::arrow::decoder::dictionary_index::DictIndexDecoder::read"], "模糊匹配": []},
    {"精确匹配": ["daft_core::datatypes::infer_datatype::InferDataType::comparison_op"], "模糊匹配": []},
    {"精确匹配": ["daft_core::datatypes::infer_datatype::InferDataType::logical_op"], "模糊匹配": []},
    {"精确匹配": ["arrow_buffer::buffer::ops::buffer_bin_or"], "模糊匹配": []},
    {"精确匹配": ["daft_schema::field::Field::new"], "模糊匹配": []},
    {"精确匹配": ["alloc::collections::btree::map::IntoIter<K,V,A>::dying_next"], "模糊匹配": []},
    {"精确匹配": ["arrow_schema::field::Field::extension_type_name"], "模糊匹配": []},
    {"精确匹配": ["core::ptr::drop_in_place<daft_dsl::expr::Expr>"], "模糊匹配": []},
    {"精确匹配": ["daft_core::series::ops::logical::<impl daft_core::array::ops::DaftLogical<&daft_core::series::Series> for daft_core::series::Series>::or"], "模糊匹配": []},
    {"精确匹配": ["daft_dsl::expr::Expr::name"], "模糊匹配": []},
    {"精确匹配": ["<parquet::arrow::array_reader::byte_array::ByteArrayReader<I> as parquet::arrow::array_reader::ArrayReader>::consume_batch"], "模糊匹配": []},
    {"精确匹配": ["daft_core::series::Series::field"], "模糊匹配": []},
    {"精确匹配": ["daft_core::lit::Literal::get_type"], "模糊匹配": []},
    {"精确匹配": ["daft_stats::table_stats::TableStatistics::eval_expression"], "模糊匹配": []},
    {"精确匹配": ["core::ptr::drop_in_place<arrow_array::array::byte_array::GenericByteArray<arrow_array::types::GenericBinaryType<i64>>>"], "模糊匹配": []},
    {"精确匹配": ["daft_core::array::ops::comparison::<impl daft_core::array::ops::DaftLogical<&daft_core::array::DataArray<daft_core::datatypes::BooleanType>> for daft_core::array::DataArray<daft_core::datatypes::BooleanType>>::or"], "模糊匹配": []},
    {"精确匹配": ["arrow_ord::cmp::compare_op::{{closure}}"], "模糊匹配": []},
    {"精确匹配": ["core::ops::function::impls::<impl core::ops::function::FnMut<A> for &mut F>::call_mut"], "模糊匹配": []},
    {"精确匹配": ["std::io::default_read_to_end::small_probe_read"], "模糊匹配": []},
    {"精确匹配": ["daft_local_execution::join::inner_join::probe_inner"], "模糊匹配": []},
    {"精确匹配": ["daft_local_execution::dynamic_batching::dyn_strategy::DynBatchingStrategy::new::{{closure}}::{{closure}}"], "模糊匹配": []},
    {"精确匹配": ["daft_core::array::ops::comparison::<impl daft_core::array::ops::DaftLogical<&daft_core::array::DataArray<daft_core::datatypes::BooleanType>> for daft_core::array::DataArray<daft_core::datatypes::BooleanType>>::and"], "模糊匹配": []},
    {"精确匹配": ["daft_core::array::from::<impl daft_core::array::DataArray<T>>::from_iter"], "模糊匹配": []},
    {"精确匹配": ["tokio::util::idle_notified_set::EntryInOneOfTheLists<T>::remove"], "模糊匹配": []},
    {"精确匹配": ["arrow_array::array::byte_array::<impl core::convert::From<arrow_array::array::byte_array::GenericByteArray<T>> for arrow_data::data::ArrayData>::from"], "模糊匹配": []},
    {"精确匹配": ["arrow_array::array::boolean_array::<impl core::convert::From<arrow_array::array::boolean_array::BooleanArray> for arrow_data::data::ArrayData>::from"], "模糊匹配": []},
    {"精确匹配": ["<alloc::sync::Arc<dyn arrow_array::array::Array> as arrow_array::array::Array>::data_type"], "模糊匹配": []},
    {"精确匹配": ["common_metrics::meters::Counter::addd"], "模糊匹配": []}
]

# ===========================================


def run_simulation():

    if not os.path.exists(INPUT_FILE):
        print(f"找不到输入文件: {INPUT_FILE}")
        return

    df_raw = pd.read_excel(INPUT_FILE, sheet_name=SHEET_NAME)

    # 自动识别 Case 列
    CASE_COL = None
    for c in ["Case", "测试用例", "测试_用例"]:
        if c in df_raw.columns:
            CASE_COL = c
            break

    if CASE_COL is None:
        raise ValueError("找不到 Case 列")

    num_cols = [
        "920B耗时(ms)优化前的",
        "9654耗时(ms)",
        "920B优化标准库到持平后的最终时间"
    ]

    for col in num_cols:
        df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce").fillna(0)

    # ======================================
    # 原始 TPCH 几何平均
    # ======================================

    df_perf = df_raw[df_raw["劣化接口"] == "性能用例"].copy()

    base_ratios = (
        df_perf["9654耗时(ms)"] /
        df_perf["920B耗时(ms)优化前的"].replace(0, np.nan)
    ).dropna()

    gmean_before = np.exp(np.log(base_ratios).mean())

    results = []

    # ======================================
    # 模拟优化
    # ======================================

    for group in OPTIMIZATION_GROUPS:

        exacts = set(group.get("精确匹配", []))
        fuzzies = group.get("模糊匹配", [])

        display_name = " + ".join(list(exacts))

        new_case_ratios = []
        q_detail_list = []

        for case_name, g_data in df_raw.groupby(CASE_COL):

            perf_row = g_data[g_data["劣化接口"] == "性能用例"]

            if perf_row.empty:
                continue

            perf_row = perf_row.iloc[0]

            orig_total_920b = perf_row["920B耗时(ms)优化前的"]
            x86_time = perf_row["9654耗时(ms)"]

            atom_rows = g_data[g_data["劣化接口"] != "性能用例"]

            total_saved = 0

            interface_920 = None
            interface_x86 = None

            for _, row in atom_rows.iterrows():

                itf = str(row["劣化接口"])

                is_hit = (itf in exacts) or any(f in itf for f in fuzzies)

                if is_hit:

                    save = row["920B耗时(ms)优化前的"] - row["9654耗时(ms)"]

                    total_saved += max(0, save)

                    interface_920 = row["920B耗时(ms)优化前的"]
                    interface_x86 = row["9654耗时(ms)"]

            # ===== 原逻辑（用于列1~3）=====
            new_920b_time = max(1e-9, orig_total_920b - total_saved)

            new_ratio = x86_time / new_920b_time

            new_case_ratios.append(new_ratio)

            # ===== 新逻辑（列4）=====
            if interface_920 is not None:

                old_ratio = x86_time / orig_total_920b

                simulated_920b_time = orig_total_920b - interface_920 + interface_x86

                new_ratio_single = x86_time / simulated_920b_time

                improvement = new_ratio_single - old_ratio

                rounded = round(improvement * 100, 2)

                if rounded > 0:
                    q_detail_list.append((case_name, rounded))

        if len(new_case_ratios) == 0:
            continue

        gmean_after = np.exp(np.log(new_case_ratios).mean())

        # ===== 排序并格式化输出 =====
        if q_detail_list:

            q_detail_list.sort(key=lambda x: x[1], reverse=True)

            formatted = [
                f"{q}（{v:.2f}%）" for q, v in q_detail_list
            ]

            detail_sentence = "支撑Tpch benchmark提升： " + " ，".join(formatted)

        else:
            detail_sentence = ""

        results.append({
            "接口名": display_name,
            "1. 改之前几何平均": gmean_before,
            "2. 优化后几何平均": gmean_after,
            "3. 性能比提升绝对值": gmean_after - gmean_before,
            "4. 对Tpch benchmark提升详情": detail_sentence
        })

    df_res = pd.DataFrame(results)

    # ======================================
    # 写Excel
    # ======================================

    with pd.ExcelWriter(OUTPUT_REPORT, engine="openpyxl") as writer:

        df_res.to_excel(writer, index=False, sheet_name="Impact")

        ws = writer.sheets["Impact"]

        percent_fmt = "0.00000000%"

        for r in range(2, len(df_res) + 2):

            ws.cell(row=r, column=2).number_format = percent_fmt
            ws.cell(row=r, column=3).number_format = percent_fmt
            ws.cell(row=r, column=4).number_format = percent_fmt

    print("模拟完成")
    print(f"原始TPCH几何平均性能比: {gmean_before:.8%}")

    if not df_res.empty:
        best = df_res["2. 优化后几何平均"].max()
        print(f"最高模拟性能比: {best:.8%}")

    print(f"结果写入: {OUTPUT_REPORT}")


def main():
    run_simulation()


if __name__ == "__main__":
    main()